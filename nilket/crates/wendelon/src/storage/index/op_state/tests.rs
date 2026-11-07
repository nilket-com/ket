//! ADR 0697 §5.6: allocation, handoff and the row invariant on a real store.

use super::*;
use crate::consensus::slab_ops::{RetireOp, TierGeneration, TierOp};
use crate::consensus::{OwnerRef, TieredSlab};
use crate::schema::{ManifestEpoch, SupersessionMetadata, TombstoneMetadata};

fn owner() -> OwnerRef {
	OwnerRef::for_test(1, 0xA)
}

fn tier_command(slab: u64) -> SlabCommand {
	SlabCommand::Tier(TierOp {
		tier: TieredSlab {
			slab: owner().slab(SlabId::new(slab)),
			l2_uri: format!("l2://{slab}"),
			payload_hash: [1; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		},
		replaces: None,
	})
}

fn retire_command(slab: u64) -> SlabCommand {
	let SlabCommand::Tier(op) = tier_command(slab) else {
		unreachable!()
	};
	SlabCommand::Retire(RetireOp {
		slab: op.tier.slab,
		expected: TierGeneration::from(&op.tier),
	})
}

async fn fresh() -> (crate::test_support::TestDir, PathBuf, PersistentLocalIndex) {
	let dir = crate::test_support::TestDir::new("wendelon-op-state");
	let path = dir.join("index.redb");
	let index = PersistentLocalIndex::initialize_new(path.clone(), None)
		.await
		.unwrap();
	(dir, path, index)
}

async fn reopen(path: &Path) -> std::io::Result<PersistentLocalIndex> {
	PersistentLocalIndex::open(path.to_path_buf()).await
}

async fn allocate_one(index: &PersistentLocalIndex, slab: u64) -> OpTicket {
	index
		.allocate_ops(vec![tier_command(slab)], None)
		.await
		.unwrap()
		.first
}

fn backlog(error: &Error) -> TerminalBacklog {
	*error
		.get_ref()
		.and_then(|cause| cause.downcast_ref::<TerminalBacklog>())
		.unwrap_or_else(|| panic!("expected TerminalBacklog, got {error}"))
}

#[tokio::test]
async fn a_new_store_allocates_from_one_and_a_reopen_never_reuses_a_sequence() {
	let (_dir, path, index) = fresh().await;
	let first = allocate_one(&index, 1).await;
	assert_eq!(first.key().op_seq, 1);
	assert_eq!(first.key().owner, owner());
	assert_eq!(first.digest(), first.command().digest());
	assert_eq!(allocate_one(&index, 2).await.key().op_seq, 2);
	drop(index);

	// A crash between allocation and use: the row survives, its sequence is
	// not reused, and it loads as its own original ticket.
	let index = reopen(&path).await.unwrap();
	let loaded = index.load_op_intents().await.unwrap();
	assert_eq!(loaded.len(), 2);
	assert_eq!(loaded[0], LoadedOp::Submittable(first));
	assert_eq!(allocate_one(&index, 3).await.key().op_seq, 3);
}

/// §5.2: the whole span and its digests in one transaction, the later op
/// reserved until its own durable transition.
#[tokio::test]
async fn a_workflow_reserves_its_whole_span_and_a_later_op_transitions_before_submission() {
	let (_dir, path, index) = fresh().await;
	let allocation = index
		.allocate_ops(vec![tier_command(1), retire_command(1)], None)
		.await
		.unwrap();
	assert_eq!(allocation.first.key().op_seq, 1);
	assert_eq!(allocation.later.len(), 1);
	let reserved = allocation.later[0].clone();
	assert_eq!(reserved.key().op_seq, 2);
	assert_eq!(
		index.oldest_op_holder().await.unwrap(),
		Some(OpHolder {
			op_seq: 1,
			transition: Some("tier"),
			state: OpHolderState::MayBeSubmitted
		})
	);
	drop(index);

	// After a crash it still loads reserved: no ticket exists for it.
	let index = reopen(&path).await.unwrap();
	let loaded = index.load_op_intents().await.unwrap();
	assert_eq!(loaded[1], LoadedOp::Reserved(reserved.clone()));

	let ticket = index.mark_may_be_submitted(reserved.clone()).await.unwrap();
	assert_eq!(ticket.key(), reserved.key());
	assert_eq!(ticket.command(), &retire_command(1), "immutable");
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert_eq!(
		index.load_op_intents().await.unwrap()[1],
		LoadedOp::Submittable(ticket)
	);
}

#[tokio::test]
async fn a_linked_intent_commits_with_its_ops_and_a_job_collision_leaves_neither() {
	let (_dir, _path, index) = fresh().await;
	let commit = crate::storage::lsm::CompactionCommit::gc_mark_delete_pending(
		owner(),
		crate::schema::CompactionJobId::new(5),
		ManifestEpoch::new(3),
		vec![SlabId::new(1)],
	);
	let intent = LsmInstallIntent {
		kind: LsmInstallKind::Control,
		phase: LsmInstallPhase::Prepared,
		local_input_rows_digest: [0; 32],
		local_output_rows_digest: [0; 32],
		local_input_runs: Vec::new(),
		local_input_files: Vec::new(),
		local_output_files: Vec::new(),
		commit_digest: *blake3::hash(&serde_json::to_vec(&commit).unwrap()).as_bytes(),
		commit: commit.clone(),
		ops: Vec::new(),
		inputs: Vec::new(),
	};
	let allocation = index
		.allocate_ops(
			vec![SlabCommand::Compaction(commit.clone())],
			Some(intent.clone()),
		)
		.await
		.unwrap();
	let snapshot = index.load_snapshot().await.unwrap();
	assert_eq!(snapshot.lsm_install_intents.len(), 1);
	let stored = snapshot.lsm_install_intents[0].clone();
	assert_eq!(stored.ops, vec![allocation.first.key()]);

	// The job is taken: nothing of this transaction commits.
	let error = index
		.allocate_ops(vec![SlabCommand::Compaction(commit)], Some(intent.clone()))
		.await
		.unwrap_err();
	assert_eq!(error.kind(), ErrorKind::AlreadyExists);
	assert_eq!(index.load_op_intents().await.unwrap().len(), 1);
	assert_eq!(allocate_one(&index, 9).await.key().op_seq, 2);

	// The handoff clears exactly its own linked intent, atomically.
	let mut other_attempt = stored.clone();
	other_attempt.ops = vec![OpKey {
		owner: owner(),
		op_seq: 99,
	}];
	assert!(
		index
			.hand_off_op(
				allocation.first.key(),
				allocation.first.digest(),
				Some(other_attempt)
			)
			.await
			.is_err(),
		"another attempt's record is not cleared"
	);
	assert_eq!(
		index
			.load_snapshot()
			.await
			.unwrap()
			.lsm_install_intents
			.len(),
		1
	);
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![]);
	index
		.hand_off_op(
			allocation.first.key(),
			allocation.first.digest(),
			Some(stored),
		)
		.await
		.unwrap();
	assert!(
		index
			.load_snapshot()
			.await
			.unwrap()
			.lsm_install_intents
			.is_empty()
	);
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(1, false)]);
}

/// §5.6's lifecycle: Intent, then AckPending, then acked, then gone only when
/// a canonical prefix covers it.
#[tokio::test]
async fn a_row_moves_from_intent_to_ack_pending_and_only_a_canonical_prefix_deletes_it() {
	let (_dir, path, index) = fresh().await;
	let ticket = allocate_one(&index, 1).await;
	let wrong_digest = index.hand_off_op(ticket.key(), [9; 32], None).await;
	assert!(wrong_digest.is_err(), "a handoff must name its own op");
	index
		.hand_off_op(ticket.key(), ticket.digest(), None)
		.await
		.unwrap();
	assert!(index.load_op_intents().await.unwrap().is_empty());
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(1, false)]);
	assert!(
		index
			.hand_off_op(ticket.key(), ticket.digest(), None)
			.await
			.is_err(),
		"a second handoff has no intent"
	);

	// A crash between the handoff and the Ack: the row is re-driven.
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(1, false)]);

	index.mark_ops_acked(vec![1]).await.unwrap();
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(1, true)]);
	assert_eq!(
		index.oldest_op_holder().await.unwrap(),
		Some(OpHolder {
			op_seq: 1,
			transition: None,
			state: OpHolderState::AwaitingAck { acked: true }
		})
	);

	// A crash after the Ack, before the prefix read: acked is not gone.
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(1, true)]);
	index.advance_known_acked_through(1).await.unwrap();
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![]);
	assert_eq!(index.oldest_op_holder().await.unwrap(), None);

	// A stale prefix changes nothing; and the next sequence is not reused.
	index.advance_known_acked_through(0).await.unwrap();
	assert_eq!(allocate_one(&index, 2).await.key().op_seq, 2);
	drop(index);
	reopen(&path)
		.await
		.expect("the invariant holds after a prefix");
}

#[tokio::test]
async fn a_prefix_past_an_open_intent_or_the_allocated_span_is_an_anomaly_that_changes_nothing() {
	let (_dir, _path, index) = fresh().await;
	let first = allocate_one(&index, 1).await;
	let second = allocate_one(&index, 2).await;
	index
		.hand_off_op(second.key(), second.digest(), None)
		.await
		.unwrap();
	for canonical in [2, 3] {
		let error = index
			.advance_known_acked_through(canonical)
			.await
			.unwrap_err();
		assert!(is_op_state_invalid(&error), "{error}");
	}
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(2, false)]);
	assert_eq!(
		index.load_op_intents().await.unwrap(),
		vec![LoadedOp::Submittable(first)]
	);
}

/// §9.3 R1 on the local side: the bound is measured from the local prefix, is
/// checked before anything is written, names its holder, and a drain still runs.
#[tokio::test]
async fn the_backlog_bound_refuses_before_insertion_names_its_holder_and_drain_resumes_it() {
	let (_dir, _path, index) = fresh().await;
	let hole = allocate_one(&index, 0).await;
	for slab in 1..OPS_WINDOW_W {
		let ticket = allocate_one(&index, slab).await;
		index
			.hand_off_op(ticket.key(), ticket.digest(), None)
			.await
			.unwrap();
	}
	let seqs: Vec<u64> = (2..=OPS_WINDOW_W).collect();
	index.mark_ops_acked(seqs).await.unwrap();

	let error = index
		.allocate_ops(vec![tier_command(5000)], None)
		.await
		.unwrap_err();
	let refused = backlog(&error);
	assert_eq!(refused.known_acked_through, 0);
	assert_eq!(refused.next_op_seq, OPS_WINDOW_W + 1);
	assert_eq!(
		refused.holder,
		Some(OpHolder {
			op_seq: 1,
			transition: Some("tier"),
			state: OpHolderState::MayBeSubmitted
		}),
		"status names the oldest unresolved sequence"
	);
	assert_eq!(
		index.load_op_intents().await.unwrap().len(),
		1,
		"no insertion"
	);

	// Draining needs no new sequence: the hole hands off, and the prefix
	// advances over the whole acked run.
	index
		.hand_off_op(hole.key(), hole.digest(), None)
		.await
		.unwrap();
	index.mark_ops_acked(vec![1]).await.unwrap();
	index
		.advance_known_acked_through(OPS_WINDOW_W)
		.await
		.unwrap();
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![]);
	assert_eq!(
		allocate_one(&index, 5000).await.key().op_seq,
		OPS_WINDOW_W + 1,
		"admission resumes"
	);
}

/// A span that would straddle the bound is refused whole, for every size.
#[tokio::test]
async fn a_span_that_does_not_fit_is_refused_whole() {
	let (_dir, _path, index) = fresh().await;
	for slab in 0..OPS_WINDOW_W - 1 {
		allocate_one(&index, slab).await;
	}
	let error = index
		.allocate_ops(vec![tier_command(1), retire_command(1)], None)
		.await
		.unwrap_err();
	assert_eq!(backlog(&error).requested, 2);
	assert_eq!(
		index.load_op_intents().await.unwrap().len() as u64,
		OPS_WINDOW_W - 1
	);
	assert_eq!(
		allocate_one(&index, 1).await.key().op_seq,
		OPS_WINDOW_W,
		"the last slot still fits, and no sequence was consumed"
	);
}

/// Absent or partial metadata is never a fresh sequence space, and a broken
/// row invariant refuses before recovery.
#[tokio::test]
async fn an_existing_store_with_absent_partial_or_inconsistent_op_state_is_refused() {
	for key in [
		META_OP_STATE_VERSION,
		META_NEXT_OP_SEQ,
		META_KNOWN_ACKED_THROUGH,
	] {
		let (_dir, path, index) = fresh().await;
		allocate_one(&index, 1).await;
		drop(index);
		remove_op_state_meta_for_test(&path, key);
		let error = reopen(&path).await.err().expect("refused");
		assert!(is_op_state_invalid(&error), "{key}: {error}");
	}

	// A sequence in the span with no row.
	let (_dir, path, index) = fresh().await;
	allocate_one(&index, 1).await;
	drop(index);
	{
		let db = Database::open(&path).unwrap();
		let tx = db.begin_write().unwrap();
		tx.open_table(OP_INTENTS_TABLE).unwrap().remove(&1).unwrap();
		tx.commit().unwrap();
	}
	assert!(is_op_state_invalid(
		&reopen(&path).await.err().expect("refused")
	));

	// A sequence with two rows.
	let (_dir, path, index) = fresh().await;
	allocate_one(&index, 1).await;
	drop(index);
	{
		let db = Database::open(&path).unwrap();
		let tx = db.begin_write().unwrap();
		tx.open_table(OP_ACK_PENDING_TABLE)
			.unwrap()
			.insert(&1, &(false, [0; 32]))
			.unwrap();
		tx.commit().unwrap();
	}
	assert!(is_op_state_invalid(
		&reopen(&path).await.err().expect("refused")
	));

	// A row outside the span.
	let (_dir, path, index) = fresh().await;
	drop(index);
	{
		let db = Database::open(&path).unwrap();
		let tx = db.begin_write().unwrap();
		tx.open_table(OP_ACK_PENDING_TABLE)
			.unwrap()
			.insert(&7, &(false, [0; 32]))
			.unwrap();
		tx.commit().unwrap();
	}
	assert!(is_op_state_invalid(
		&reopen(&path).await.err().expect("refused")
	));
}

/// §5.6 R7: at each shared transaction's commit boundary, either the pre-state
/// remains or the whole committed state is there after a reopen. A caller that
/// never learned the result finds no reused sequence and no lost row.
#[tokio::test]
async fn an_allocation_commit_fault_leaves_the_pre_state_or_the_whole_committed_state() {
	// Nothing committed.
	let (_dir, path, index) = fresh().await;
	index.cut_next_op_txn(OpTxnCut::BeforeCommit);
	assert!(
		index
			.allocate_ops(vec![tier_command(1), retire_command(1)], None)
			.await
			.is_err()
	);
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert!(index.load_op_intents().await.unwrap().is_empty());
	assert_eq!(allocate_one(&index, 9).await.key().op_seq, 1);

	// Committed; the caller got an error and holds no ticket. The reopen finds
	// the whole span, and the next allocation does not reuse it.
	let (_dir, path, index) = fresh().await;
	index.cut_next_op_txn(OpTxnCut::AfterCommit);
	assert!(
		index
			.allocate_ops(vec![tier_command(1), retire_command(1)], None)
			.await
			.is_err()
	);
	drop(index);
	let index = reopen(&path).await.unwrap();
	let loaded = index.load_op_intents().await.unwrap();
	assert_eq!(loaded.len(), 2);
	assert!(matches!(&loaded[0], LoadedOp::Submittable(ticket) if ticket.key().op_seq == 1));
	assert!(matches!(&loaded[1], LoadedOp::Reserved(reserved) if reserved.key().op_seq == 2));
	assert_eq!(allocate_one(&index, 9).await.key().op_seq, 3);
}

#[tokio::test]
async fn a_handoff_commit_fault_leaves_the_intent_or_the_whole_ack_pending_state() {
	let (_dir, path, index) = fresh().await;
	let ticket = allocate_one(&index, 1).await;
	index.cut_next_op_txn(OpTxnCut::BeforeCommit);
	assert!(
		index
			.hand_off_op(ticket.key(), ticket.digest(), None)
			.await
			.is_err()
	);
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert_eq!(
		index.load_op_intents().await.unwrap(),
		vec![LoadedOp::Submittable(ticket.clone())]
	);
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![]);

	index.cut_next_op_txn(OpTxnCut::AfterCommit);
	assert!(
		index
			.hand_off_op(ticket.key(), ticket.digest(), None)
			.await
			.is_err()
	);
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert!(index.load_op_intents().await.unwrap().is_empty());
	assert_eq!(
		index.load_ack_pending().await.unwrap(),
		vec![(1, false)],
		"no pending Ack is lost"
	);
	assert_eq!(allocate_one(&index, 2).await.key().op_seq, 2);
}

#[tokio::test]
async fn a_reserved_transition_or_ack_commit_fault_keeps_the_invariant() {
	let (_dir, path, index) = fresh().await;
	let allocation = index
		.allocate_ops(vec![tier_command(1), retire_command(1)], None)
		.await
		.unwrap();
	let reserved = allocation.later[0].clone();
	index.cut_next_op_txn(OpTxnCut::AfterCommit);
	assert!(index.mark_may_be_submitted(reserved).await.is_err());
	drop(index);
	let index = reopen(&path).await.unwrap();
	let loaded = index.load_op_intents().await.unwrap();
	assert!(
		matches!(&loaded[1], LoadedOp::Submittable(ticket) if ticket.key().op_seq == 2),
		"the transition committed even though its caller never learned"
	);

	let first = allocation.first;
	index
		.hand_off_op(first.key(), first.digest(), None)
		.await
		.unwrap();
	index.cut_next_op_txn(OpTxnCut::BeforeCommit);
	assert!(index.mark_ops_acked(vec![1]).await.is_err());
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(1, false)]);
	index.mark_ops_acked(vec![1]).await.unwrap();
	index.cut_next_op_txn(OpTxnCut::AfterCommit);
	assert!(index.advance_known_acked_through(1).await.is_err());
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![]);
	index.advance_known_acked_through(1).await.unwrap();
	assert_eq!(allocate_one(&index, 2).await.key().op_seq, 3);
}

/// R4: a rejected candidate's identity survives its Intent, atomically: at the
/// handoff's commit boundary there is always one durable record of it.
#[tokio::test]
async fn a_rejected_tier_handoff_keeps_its_candidate_durable_at_every_cut() {
	let (_dir, path, index) = fresh().await;
	let ticket = allocate_one(&index, 1).await;
	index.cut_next_op_txn(OpTxnCut::BeforeCommit);
	assert!(index.hand_off_rejected_tier(&ticket).await.is_err());
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert_eq!(index.load_op_intents().await.unwrap().len(), 1);
	assert!(
		index
			.load_rejected_tier_candidates()
			.await
			.unwrap()
			.is_empty()
	);

	index.cut_next_op_txn(OpTxnCut::AfterCommit);
	assert!(index.hand_off_rejected_tier(&ticket).await.is_err());
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert!(index.load_op_intents().await.unwrap().is_empty());
	assert_eq!(index.load_ack_pending().await.unwrap(), vec![(1, false)]);
	assert_eq!(
		index.load_rejected_tier_candidates().await.unwrap(),
		vec![RejectedTierCandidate {
			op_seq: 1,
			slab_id: SlabId::new(1),
			l2_uri: "l2://1".into(),
			payload_hash: [1; 32],
		}]
	);
}

/// R5: a mark's handoff leaves its reserved removal the input authority its
/// linked intent held, in the same transaction, and it survives a reopen.
#[tokio::test]
async fn a_mark_handoff_leaves_its_reserved_removal_the_input_authority() {
	let (_dir, path, index) = fresh().await;
	let commit = crate::storage::lsm::CompactionCommit::gc_mark_delete_pending(
		owner(),
		crate::schema::CompactionJobId::new(5),
		ManifestEpoch::new(3),
		vec![SlabId::new(1)],
	);
	let intent = LsmInstallIntent {
		kind: LsmInstallKind::Control,
		phase: LsmInstallPhase::Prepared,
		local_input_rows_digest: [0; 32],
		local_output_rows_digest: [0; 32],
		local_input_runs: Vec::new(),
		local_input_files: Vec::new(),
		local_output_files: Vec::new(),
		commit_digest: *blake3::hash(&serde_json::to_vec(&commit).unwrap()).as_bytes(),
		commit: commit.clone(),
		ops: Vec::new(),
		inputs: Vec::new(),
	};
	let allocation = index
		.allocate_ops(
			vec![SlabCommand::Compaction(commit), retire_command(1)],
			Some(intent),
		)
		.await
		.unwrap();
	let authority = vec![LsmInputAuthority {
		opened: 0,
		slab_id: SlabId::new(1),
		revision: 7,
		tiered: None,
	}];
	let removal = allocation.later[0].key();
	assert!(
		LoadedOp::Reserved(allocation.later[0].clone()).awaits_its_workflow(),
		"a reserved removal cannot run before its mark leaves its evidence"
	);
	index
		.hand_off_gc_mark(
			&allocation.first,
			allocation.linked.clone().unwrap(),
			removal,
			authority.clone(),
		)
		.await
		.unwrap();
	drop(index);
	let index = reopen(&path).await.unwrap();
	let loaded = index.load_op_intents().await.unwrap();
	assert_eq!(loaded.len(), 1);
	assert_eq!(loaded[0].reserved_inputs(), Some(authority.as_slice()));
	assert!(!loaded[0].awaits_its_workflow());
	assert!(
		index
			.load_snapshot()
			.await
			.unwrap()
			.lsm_install_intents
			.is_empty()
	);
}

/// A removal is T1's Complete: its reserved row cannot become submittable until the
/// run is durably gone, and Cancelling it after a rejected claim needs no such thing.
#[tokio::test]
async fn a_reserved_removal_cannot_be_submitted_before_its_run_is_gone() {
	let (_dir, _path, index) = fresh().await;
	let hlc = crate::schema::HybridTimestamp {
		physical_ms: 1,
		logical: 0,
		node_id: 0,
	};
	let removal = SlabCommand::GcRemove(crate::consensus::slab_ops::GcRemove {
		job_id: crate::schema::CompactionJobId::new(7),
		slab: owner().slab(SlabId::new(70)),
		expected_run: crate::consensus::slab_ops::RunIdentity {
			compaction_level: crate::schema::CompactionLevelIndex::L0,
			size_bytes: crate::schema::ByteCount::new(1),
			row_count: crate::schema::RowCount::new(1),
			min_row_hlc: hlc,
			max_row_hlc: hlc,
		},
	});
	let allocation = index
		.allocate_ops(vec![tier_command(1), removal], None)
		.await
		.unwrap();
	let reserved = allocation.later.into_iter().next().unwrap();
	assert!(is_op_state_invalid(
		&index
			.mark_may_be_submitted(reserved.clone())
			.await
			.unwrap_err()
	));
	index
		.advance_op_progress(
			OpProgressUpdate {
				key: reserved.key(),
				digest: reserved.digest(),
				progress: OpProgress::RunDropped(None),
			},
			None,
		)
		.await
		.unwrap();
	index.mark_may_be_submitted(reserved).await.unwrap();

	// A run-less Complete is held to the same order.
	let slab = SlabId::new(71);
	index.reserve_slab_ids(vec![slab], None).await.unwrap();
	index.expose_reserved_id(slab).await.unwrap();
	index
		.abandon_exposed_id(
			slab,
			AbandonedOutput {
				file: None,
				relocated: false,
			},
			None,
		)
		.await
		.unwrap();
	let (_claim, complete, _ownership) = index
		.begin_cleanup(slab, owner(), crate::schema::CompactionJobId::new(8))
		.await
		.unwrap();
	assert!(is_op_state_invalid(
		&index.mark_may_be_submitted(complete).await.unwrap_err()
	));
}

// ADR 0697 §4 T2 and T3: the reservation lifecycle and its charge.

fn ids(range: std::ops::Range<u64>) -> Vec<SlabId> {
	range.map(SlabId::new).collect()
}

fn refusal(error: &Error) -> &ReservationRefused {
	error
		.get_ref()
		.and_then(|cause| cause.downcast_ref::<ReservationRefused>())
		.unwrap_or_else(|| panic!("expected a typed refusal, got {error}"))
}

#[tokio::test]
async fn a_reservation_is_charged_before_any_row_and_refused_whole() {
	let (_dir, _path, index) = fresh().await;
	// 513 ids would promise 1026 sequences of a window of 1024.
	let error = index.reserve_slab_ids(ids(1..514), None).await.unwrap_err();
	backlog(&error);
	assert!(index.load_reserved_ids().await.unwrap().is_empty());

	index.reserve_slab_ids(ids(1..513), None).await.unwrap();
	// Every producer sees the charge, through both of its admission paths.
	backlog(&index.ensure_op_room(1).await.unwrap_err());
	backlog(
		&index
			.allocate_ops(vec![tier_command(9)], None)
			.await
			.unwrap_err(),
	);
	backlog(
		&index
			.reserve_slab_ids(ids(600..601), None)
			.await
			.unwrap_err(),
	);
}

/// §4: `S + 2C` cannot rise through a conversion, and a retry cannot spend twice.
#[tokio::test]
async fn a_charge_converts_once_and_a_full_window_still_cleans_up() {
	let (_dir, path, index) = fresh().await;
	index.reserve_slab_ids(ids(1..513), None).await.unwrap();
	let slab = SlabId::new(1);
	index.expose_reserved_id(slab).await.unwrap();
	index
		.abandon_exposed_id(
			slab,
			AbandonedOutput {
				file: None,
				relocated: false,
			},
			None,
		)
		.await
		.unwrap();
	let job = crate::schema::CompactionJobId::new(1);
	let (claim, complete, _ownership) = index.begin_cleanup(slab, owner(), job).await.unwrap();
	assert_eq!((claim.key().op_seq, complete.key().op_seq), (1, 2));
	assert!(matches!(
		refusal(&index.begin_cleanup(slab, owner(), job).await.unwrap_err()).found,
		Some(ReservedIdState::Cleaning { .. })
	));
	// One charge became two allocated sequences: still exactly full.
	backlog(&index.ensure_op_room(1).await.unwrap_err());
	// A burn at a full window spends less than it held.
	let burned = SlabId::new(2);
	let (ticket, _ownership) = index.begin_burn(burned, owner()).await.unwrap();
	assert_eq!(ticket.key().op_seq, 3);
	index.ensure_op_room(1).await.unwrap();
	drop(index);
	reopen(&path)
		.await
		.expect("the accounting validates at open");
}

/// Only a present `Unsubmitted` row exposes. Burning is permanent.
#[tokio::test]
async fn exposure_comes_only_from_unsubmitted_and_a_burn_is_never_revived() {
	let (_dir, path, index) = fresh().await;
	let (a, b) = (SlabId::new(1), SlabId::new(2));
	index.reserve_slab_ids(vec![a, b], None).await.unwrap();

	index.expose_reserved_id(a).await.unwrap();
	assert_eq!(
		refusal(&index.expose_reserved_id(a).await.unwrap_err()).found,
		Some(ReservedIdState::Exposed { abandoned: None })
	);
	// Exposed takes T1: there is no burning it.
	assert!(matches!(
		refusal(&index.begin_burn(a, owner()).await.unwrap_err()).found,
		Some(ReservedIdState::Exposed { .. })
	));

	index.begin_burn(b, owner()).await.unwrap();
	assert!(matches!(
		refusal(&index.expose_reserved_id(b).await.unwrap_err()).found,
		Some(ReservedIdState::Burning { .. })
	));
	assert_eq!(
		refusal(&index.expose_reserved_id(SlabId::new(99)).await.unwrap_err()).found,
		None
	);
	drop(index);
	let index = reopen(&path).await.unwrap();
	assert!(matches!(
		refusal(&index.expose_reserved_id(b).await.unwrap_err()).found,
		Some(ReservedIdState::Burning { .. })
	));
}

/// Each T2 transition at its commit boundary: all of it or none of it.
#[tokio::test]
async fn each_reservation_transition_commits_whole_at_either_cut() {
	for cut in [OpTxnCut::BeforeCommit, OpTxnCut::AfterCommit] {
		let committed = cut == OpTxnCut::AfterCommit;
		let (_dir, path, index) = fresh().await;
		let slab = SlabId::new(1);
		let water = index.slab_id_high_water().await.unwrap();

		// The row and the bound, one transaction.
		index.cut_next_op_txn(cut);
		assert!(
			index
				.reserve_slab_ids(vec![slab], Some(water + 64))
				.await
				.is_err()
		);
		drop(index);
		let index = reopen(&path).await.unwrap();
		assert_eq!(
			index.reserved_id_state(slab).await.unwrap(),
			committed.then_some(ReservedIdState::Unsubmitted)
		);
		assert_eq!(
			index.slab_id_high_water().await.unwrap(),
			if committed { water + 64 } else { water }
		);
		if !committed {
			index.reserve_slab_ids(vec![slab], None).await.unwrap();
		}

		// The burn and its operation, one transaction.
		index.cut_next_op_txn(cut);
		assert!(index.begin_burn(slab, owner()).await.is_err());
		drop(index);
		let index = reopen(&path).await.unwrap();
		let state = index.reserved_id_state(slab).await.unwrap().unwrap();
		let ops = index.load_op_intents().await.unwrap();
		assert_eq!(
			matches!(state, ReservedIdState::Burning { .. }),
			committed,
			"{cut:?}"
		);
		assert_eq!(ops.len(), usize::from(committed), "{cut:?}");

		// Exposure, on a second id.
		let exposed = SlabId::new(2);
		index.reserve_slab_ids(vec![exposed], None).await.unwrap();
		index.cut_next_op_txn(cut);
		assert!(index.expose_reserved_id(exposed).await.is_err());
		drop(index);
		let index = reopen(&path).await.unwrap();
		assert_eq!(
			matches!(
				index.reserved_id_state(exposed).await.unwrap(),
				Some(ReservedIdState::Exposed { .. })
			),
			committed
		);
	}
}

#[tokio::test]
async fn open_refuses_reservations_that_break_the_accounting() {
	let write = |path: &Path, id: u64, state: ReservedIdState| {
		let db = Database::open(path).unwrap();
		let tx = db.begin_write().unwrap();
		tx.open_table(RESERVED_SLAB_IDS_TABLE)
			.unwrap()
			.insert(&id, postcard::to_stdvec(&state).unwrap().as_slice())
			.unwrap();
		tx.commit().unwrap();
	};
	// More promised than the window holds.
	let (_dir, path, index) = fresh().await;
	drop(index);
	for id in 1..=513 {
		write(&path, id, ReservedIdState::Unsubmitted);
	}
	assert!(is_op_state_invalid(
		&reopen(&path).await.err().expect("refused")
	));
	// A burn with no operation behind it.
	let (_dir, path, index) = fresh().await;
	drop(index);
	write(
		&path,
		1,
		ReservedIdState::Burning {
			op: OpKey {
				owner: owner(),
				op_seq: 40,
			},
		},
	);
	assert!(is_op_state_invalid(
		&reopen(&path).await.err().expect("refused")
	));
}

/// A rejected install's outputs all transfer with its handoff, or none do.
#[tokio::test]
async fn a_rejected_install_transfers_every_output_or_none() {
	let (_dir, _path, index) = fresh().await;
	let (a, b) = (SlabId::new(1), SlabId::new(2));
	index.reserve_slab_ids(vec![a, b], None).await.unwrap();
	index.expose_reserved_id(a).await.unwrap();
	let ticket = allocate_one(&index, 9).await;
	let output = AbandonedOutput {
		file: None,
		relocated: false,
	};
	// `b` was never exposed: nothing about this handoff happens.
	assert!(
		index
			.hand_off_op_with(
				ticket.key(),
				ticket.digest(),
				None,
				HandoffEvidence::AbandonOutputs(vec![(a, output.clone()), (b, output.clone())]),
				None,
			)
			.await
			.is_err()
	);
	assert_eq!(index.load_op_intents().await.unwrap().len(), 1);
	assert_eq!(
		index.reserved_id_state(a).await.unwrap(),
		Some(ReservedIdState::Exposed { abandoned: None })
	);
	index.expose_reserved_id(b).await.unwrap();
	index
		.hand_off_op_with(
			ticket.key(),
			ticket.digest(),
			None,
			HandoffEvidence::AbandonOutputs(vec![(a, output.clone()), (b, output.clone())]),
			None,
		)
		.await
		.unwrap();
	for id in [a, b] {
		assert_eq!(
			index.reserved_id_state(id).await.unwrap(),
			Some(ReservedIdState::Exposed {
				abandoned: Some(output.clone())
			})
		);
	}
}
