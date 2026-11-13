//! The ADR 0697 §9.3 protocol list at the state machine.

use super::*;
use crate::consensus::slab_ops::{GcRemove, RunIdentity, SlabCommand, SlabOp};

use crate::schema::{
	ByteCount, CompactionJobId, CompactionLevelIndex, HybridTimestamp, RowCount, SlabId,
	SupersessionMetadata, TombstoneMetadata,
};

const NODE: u32 = 1;

fn owner(incarnation: u128) -> OwnerRef {
	OwnerRef::for_test(NODE, incarnation)
}

fn tier(owner: OwnerRef, slab: u64, uri: &str) -> TieredSlab {
	TieredSlab {
		slab: owner.slab(SlabId::new(slab)),
		l2_uri: uri.into(),
		payload_hash: [7; 32],
		is_encrypted: false,
		tenant_id: None,
		tenant_kek_id: None,
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
		certified: None,
	}
}

fn first_tier(owner: OwnerRef, slab: u64) -> SlabCommand {
	SlabCommand::Tier(TierOp {
		tier: tier(owner, slab, "l2://a"),
		replaces: None,
	})
}

fn registered(incarnation: u128) -> SmState {
	let mut state = SmState::default();
	let (response, _) = apply_request_at_log_index(
		&mut state,
		WendelonRequest::RegisterIncarnation(RegisterIncarnation {
			node: NODE,
			incarnation: owner(incarnation).incarnation(),
			supersedes: None,
		}),
		1,
		NODE,
	);
	assert_eq!(response, WendelonResponse::Success);
	state
}

fn supersede(state: &mut SmState, from: u128, to: u128) {
	let (response, _) = apply_request_at_log_index(
		state,
		WendelonRequest::RegisterIncarnation(RegisterIncarnation {
			node: NODE,
			incarnation: owner(to).incarnation(),
			supersedes: Some(owner(from).incarnation()),
		}),
		2,
		NODE,
	);
	assert_eq!(response, WendelonResponse::Success);
}

fn apply_op(
	state: &mut SmState,
	op_seq: u64,
	command: SlabCommand,
	index: u64,
) -> (WendelonResponse, Option<TierObserverEvent>) {
	apply_request_at_log_index(
		state,
		WendelonRequest::SlabOp(SlabOp { op_seq, command }),
		index,
		NODE,
	)
}

fn cancel(
	state: &mut SmState,
	key: OpKey,
	target_digest: [u8; 32],
	index: u64,
) -> WendelonResponse {
	apply_request_at_log_index(
		state,
		WendelonRequest::CancelOp(CancelOp { key, target_digest }),
		index,
		NODE,
	)
	.0
}

fn ack(state: &mut SmState, owner: OwnerRef, seqs: Vec<u64>) -> WendelonResponse {
	apply_request_at_log_index(
		state,
		WendelonRequest::AckOutcomes(AckOutcomes { owner, seqs }),
		0,
		NODE,
	)
	.0
}

fn outcome(response: &WendelonResponse) -> &OpOutcome {
	match response {
		WendelonResponse::Op(OpResponse::Receipt(receipt)) => &receipt.outcome,
		other => panic!("expected a receipt, got {other:?}"),
	}
}

fn applied(index: u64) -> OpOutcome {
	OpOutcome::Recorded(RecordedOutcome::Applied { index })
}

fn rejected(reason: SlabRejection, index: u64) -> OpOutcome {
	OpOutcome::Recorded(RecordedOutcome::Rejected { reason, index })
}

fn entries(state: &SmState, owner: OwnerRef) -> usize {
	state
		.op_tables
		.get(&owner)
		.map_or(0, |table| table.outcomes.len())
}

#[test]
fn a_new_op_records_its_receipt_and_a_retry_returns_the_original_index_with_no_effect() {
	let mut state = registered(0xA);
	let command = first_tier(owner(0xA), 1);
	let (response, event) = apply_op(&mut state, 1, command.clone(), 10);
	assert_eq!(outcome(&response), &applied(10));
	assert!(matches!(event, Some(TierObserverEvent::Tiered(..))));
	let WendelonResponse::Op(OpResponse::Receipt(first)) = &response else {
		unreachable!()
	};
	assert_eq!(first.digest, command.digest());
	assert_eq!(
		first.key,
		OpKey {
			owner: owner(0xA),
			op_seq: 1
		}
	);
	let (retry, event) = apply_op(&mut state, 1, command, 99);
	assert_eq!(outcome(&retry), &applied(10), "never the retry's own index");
	assert!(event.is_none(), "a receipt has zero effects");
	assert_eq!(entries(&state, owner(0xA)), 1);
}

#[test]
fn the_same_key_with_another_digest_is_an_unrecorded_conflict() {
	let mut state = registered(0xA);
	apply_op(&mut state, 1, first_tier(owner(0xA), 1), 10);
	let before = state.tiered_slabs.clone();
	let (response, event) = apply_op(&mut state, 1, first_tier(owner(0xA), 2), 11);
	assert_eq!(outcome(&response), &OpOutcome::Conflict);
	assert!(event.is_none());
	assert_eq!(state.tiered_slabs, before);
	assert_eq!(entries(&state, owner(0xA)), 1);
}

/// §5.3 "supersession guard": history comes first, and confers no authority.
#[test]
fn a_superseded_owner_gets_its_receipt_a_conflict_or_a_refusal_and_changes_nothing() {
	let mut state = registered(0xA);
	let accepted = first_tier(owner(0xA), 1);
	apply_op(&mut state, 1, accepted.clone(), 10);
	supersede(&mut state, 0xA, 0xB);
	let tiers = state.tiered_slabs.clone();

	let (retry, event) = apply_op(&mut state, 1, accepted.clone(), 20);
	assert_eq!(outcome(&retry), &applied(10));
	assert!(event.is_none());
	let (conflict, _) = apply_op(&mut state, 1, first_tier(owner(0xA), 2), 21);
	assert_eq!(outcome(&conflict), &OpOutcome::Conflict);
	let (new, event) = apply_op(&mut state, 2, first_tier(owner(0xA), 3), 22);
	assert!(matches!(
		new,
		WendelonResponse::Refused(SlabRefusal::NotCurrentOwner { .. })
	));
	assert!(event.is_none());

	// Cancel follows the same three cases.
	let key = |op_seq| OpKey {
		owner: owner(0xA),
		op_seq,
	};
	assert_eq!(
		outcome(&cancel(&mut state, key(1), accepted.digest(), 23)),
		&applied(10)
	);
	assert_eq!(
		outcome(&cancel(&mut state, key(1), [9; 32], 24)),
		&OpOutcome::Conflict
	);
	assert!(matches!(
		cancel(&mut state, key(2), [9; 32], 25),
		WendelonResponse::Refused(SlabRefusal::NotCurrentOwner { .. })
	));

	assert_eq!(state.tiered_slabs, tiers, "no canonical effect");
	assert_eq!(
		entries(&state, owner(0xA)),
		1,
		"a retired table never grows"
	);
}

#[test]
fn only_the_owners_own_node_as_appending_leader_makes_a_new_effect() {
	let mut state = registered(0xA);
	let command = first_tier(owner(0xA), 1);
	let (response, event) = apply_request_at_log_index(
		&mut state,
		WendelonRequest::SlabOp(SlabOp {
			op_seq: 1,
			command: command.clone(),
		}),
		10,
		2,
	);
	assert_eq!(
		response,
		WendelonResponse::Refused(SlabRefusal::NotAppendingLeader {
			owner_node: NODE,
			leader_node: 2
		})
	);
	assert!(event.is_none());
	assert!(state.tiered_slabs.is_empty());
	assert_eq!(entries(&state, owner(0xA)), 0);

	// A receipt answers under any leader: it is history, not an effect.
	apply_op(&mut state, 1, command.clone(), 11);
	let (retry, _) = apply_request_at_log_index(
		&mut state,
		WendelonRequest::SlabOp(SlabOp { op_seq: 1, command }),
		12,
		2,
	);
	assert_eq!(outcome(&retry), &applied(11));
}

#[test]
fn an_unknown_owner_is_refused_and_creates_no_table() {
	let mut state = registered(0xA);
	let stranger = owner(0x77);
	let (response, _) = apply_op(&mut state, 1, first_tier(stranger, 1), 10);
	assert!(matches!(
		response,
		WendelonResponse::Refused(SlabRefusal::NotCurrentOwner { .. })
	));
	let cancelled = cancel(
		&mut state,
		OpKey {
			owner: stranger,
			op_seq: 1,
		},
		[1; 32],
		11,
	);
	assert!(matches!(
		cancelled,
		WendelonResponse::Refused(SlabRefusal::NotCurrentOwner { .. })
	));
	assert!(matches!(
		ack(&mut state, stranger, vec![1]),
		WendelonResponse::Refused(SlabRefusal::NotCurrentOwner { .. })
	));
	assert!(!state.op_tables.contains_key(&stranger));
}

#[test]
fn original_before_cancel_returns_applied_and_cancel_before_original_returns_cancelled() {
	let mut state = registered(0xA);
	let command = first_tier(owner(0xA), 1);
	let key = |op_seq| OpKey {
		owner: owner(0xA),
		op_seq,
	};
	apply_op(&mut state, 1, command.clone(), 10);
	assert_eq!(
		outcome(&cancel(&mut state, key(1), command.digest(), 11)),
		&applied(10)
	);
	assert!(
		state
			.tiered_slabs
			.contains_key(&owner(0xA).slab(SlabId::new(1)))
	);

	let late = first_tier(owner(0xA), 2);
	assert_eq!(
		outcome(&cancel(&mut state, key(2), late.digest(), 12)),
		&rejected(SlabRejection::Cancelled, 12)
	);
	let (original, event) = apply_op(&mut state, 2, late.clone(), 13);
	assert_eq!(
		outcome(&original),
		&rejected(SlabRejection::Cancelled, 12),
		"the late original learns it was cancelled"
	);
	assert!(event.is_none());
	assert!(
		!state
			.tiered_slabs
			.contains_key(&owner(0xA).slab(SlabId::new(2)))
	);
	// Idempotent; and a Cancel of another digest is the anomaly.
	assert_eq!(
		outcome(&cancel(&mut state, key(2), late.digest(), 14)),
		&rejected(SlabRejection::Cancelled, 12)
	);
	assert_eq!(
		outcome(&cancel(&mut state, key(2), [9; 32], 15)),
		&OpOutcome::Conflict
	);
}

/// Out of order: 3 acked before 2. The prefix stops at the hole, then advances
/// over the whole run and prunes only then.
#[test]
fn the_prefix_never_crosses_an_undecided_or_unacked_sequence() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	for seq in 1..=3 {
		apply_op(&mut state, seq, first_tier(own, seq), 10 + seq);
	}
	let acked = |response: WendelonResponse| match response {
		WendelonResponse::Op(OpResponse::Ack(receipt)) => receipt,
		other => panic!("expected an ack receipt, got {other:?}"),
	};
	let receipt = acked(ack(&mut state, own, vec![1, 3]));
	assert_eq!(receipt.accepted, vec![1, 3]);
	assert_eq!(receipt.acked_through, 1);
	assert_eq!(
		entries(&state, own),
		2,
		"3 is not pruned past the hole at 2"
	);

	let receipt = acked(ack(&mut state, own, vec![2]));
	assert_eq!(receipt.acked_through, 3);
	assert_eq!(entries(&state, own), 0);

	// Undecided, past the window and already covered, in one partial batch.
	let receipt = acked(ack(&mut state, own, vec![1, 4, 3 + OPS_WINDOW_W + 1]));
	assert_eq!(receipt.owner, own);
	assert_eq!(receipt.accepted, vec![1]);
	assert_eq!(receipt.not_decided, vec![4]);
	assert_eq!(receipt.outside_window, vec![3 + OPS_WINDOW_W + 1]);
	assert_eq!(receipt.acked_through, 3, "an undecided seq moves nothing");

	assert_eq!(
		ack(&mut state, own, vec![1; OPS_WINDOW_W as usize + 1]),
		WendelonResponse::Refused(SlabRefusal::AckBatchTooLarge {
			len: OPS_WINDOW_W + 1
		})
	);
}

#[test]
fn a_duplicate_after_its_ack_is_unavailable_live_and_after_a_snapshot_round_trip() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	let command = first_tier(own, 1);
	apply_op(&mut state, 1, command.clone(), 10);
	ack(&mut state, own, vec![1]);
	let tiers = state.tiered_slabs.clone();

	let (duplicate, event) = apply_op(&mut state, 1, command.clone(), 11);
	assert_eq!(outcome(&duplicate), &OpOutcome::Unavailable);
	assert!(event.is_none());

	let (_, mut restored) = decode_snapshot_reader(&encode_snapshot(&state).unwrap()[..]).unwrap();
	assert_eq!(restored.op_tables, state.op_tables);
	let (duplicate, event) = apply_op(&mut restored, 1, command, 12);
	assert_eq!(outcome(&duplicate), &OpOutcome::Unavailable);
	assert!(event.is_none());
	assert_eq!(restored.tiered_slabs, tiers);
	assert_eq!(
		entries(&restored, own),
		0,
		"an unavailable answer records nothing"
	);
}

/// §9.3 R1: keep seq 1 open and complete the rest of the window. Nothing past
/// the window records, whatever arrives; the hole closes through Cancel, Ack and
/// a replay still work when full, and admission resumes.
#[test]
fn a_held_prefix_bounds_the_window_and_it_still_drains() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	for seq in 2..=OPS_WINDOW_W {
		let (response, _) = apply_op(&mut state, seq, first_tier(own, seq), seq);
		assert_eq!(outcome(&response), &applied(seq));
	}
	let seqs: Vec<u64> = (2..=OPS_WINDOW_W).collect();
	ack(&mut state, own, seqs);
	assert_eq!(
		state.op_tables[&own].acked_through, 0,
		"seq 1 holds the prefix"
	);
	let held = entries(&state, own);

	for attempt in 0..5 {
		let beyond = OPS_WINDOW_W + 1 + attempt;
		let (response, event) = apply_op(&mut state, beyond, first_tier(own, beyond), beyond);
		assert_eq!(
			response,
			WendelonResponse::Refused(SlabRefusal::OutsideWindow {
				key: OpKey {
					owner: own,
					op_seq: beyond
				},
				acked_through: 0
			})
		);
		assert!(event.is_none());
		let key = OpKey {
			owner: own,
			op_seq: beyond,
		};
		assert!(matches!(
			cancel(&mut state, key, [1; 32], beyond),
			WendelonResponse::Refused(SlabRefusal::OutsideWindow { .. })
		));
		assert_eq!(
			entries(&state, own),
			held,
			"nothing outside the window records"
		);
	}
	assert!(entries(&state, own) as u64 <= OPS_WINDOW_W);

	// Draining: a replay inside the full window, then Cancel of the hole.
	let (replay, _) = apply_op(&mut state, 2, first_tier(own, 2), 5000);
	assert_eq!(outcome(&replay), &applied(2));
	let key = OpKey {
		owner: own,
		op_seq: 1,
	};
	assert_eq!(
		outcome(&cancel(&mut state, key, [1; 32], 5001)),
		&rejected(SlabRejection::Cancelled, 5001)
	);
	ack(&mut state, own, vec![1]);
	assert_eq!(state.op_tables[&own].acked_through, OPS_WINDOW_W);
	assert_eq!(entries(&state, own), 0);

	let next = OPS_WINDOW_W + 1;
	let (response, _) = apply_op(&mut state, next, first_tier(own, next), 5002);
	assert_eq!(outcome(&response), &applied(5002), "admission resumes");
}

#[test]
fn a_tier_or_retire_of_a_stale_generation_is_rejected_and_records_that() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	let generation = |uri| TierGeneration::from(&tier(own, 1, uri));
	let retire = |expected| {
		SlabCommand::Retire(RetireOp {
			slab: own.slab(SlabId::new(1)),
			expected,
			certified: None,
		})
	};
	let mismatch = |expected, current| SlabRejection::GenerationMismatch { expected, current };

	// Retiring a slab with no pointer.
	let (response, event) = apply_op(&mut state, 1, retire(generation("l2://a")), 10);
	assert_eq!(outcome(&response), &rejected(SlabRejection::NotTiered, 10));
	assert!(event.is_none());

	// First tier, then a delayed first tier of another object.
	apply_op(&mut state, 2, first_tier(own, 1), 11);
	let stale_first = SlabCommand::Tier(TierOp {
		tier: tier(own, 1, "l2://other"),
		replaces: None,
	});
	let (response, event) = apply_op(&mut state, 3, stale_first, 12);
	assert_eq!(
		outcome(&response),
		&rejected(mismatch(None, Some(generation("l2://a"))), 12)
	);
	assert!(event.is_none());

	// B replaces A; a delayed retirement of A leaves B.
	let replacement = SlabCommand::Tier(TierOp {
		tier: tier(own, 1, "l2://b"),
		replaces: Some(generation("l2://a")),
	});
	let (response, event) = apply_op(&mut state, 4, replacement, 13);
	assert_eq!(outcome(&response), &applied(13));
	assert!(event.is_some());
	let (response, event) = apply_op(&mut state, 5, retire(generation("l2://a")), 14);
	assert_eq!(
		outcome(&response),
		&rejected(
			mismatch(Some(generation("l2://a")), Some(generation("l2://b"))),
			14
		)
	);
	assert!(event.is_none());
	assert_eq!(
		state.tiered_slabs[&own.slab(SlabId::new(1))].l2_uri,
		"l2://b"
	);

	// The exact generation retires.
	let (response, event) = apply_op(&mut state, 6, retire(generation("l2://b")), 15);
	assert_eq!(outcome(&response), &applied(15));
	assert!(matches!(event, Some(TierObserverEvent::Retired(..))));
	assert!(state.tiered_slabs.is_empty());
}

/// Advisory metadata is not identity: a refresh of the same generation applies.
#[test]
fn a_metadata_refresh_names_the_same_generation() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	apply_op(&mut state, 1, first_tier(own, 1), 10);
	let mut refreshed = tier(own, 1, "l2://a");
	refreshed.tombstone_metadata = TombstoneMetadata::Untracked;
	assert_eq!(
		TierGeneration::from(&refreshed),
		TierGeneration::from(&tier(own, 1, "l2://a"))
	);
	let (response, event) = apply_op(
		&mut state,
		2,
		SlabCommand::Tier(TierOp {
			tier: refreshed.clone(),
			replaces: Some(TierGeneration::from(&refreshed)),
		}),
		11,
	);
	assert_eq!(outcome(&response), &applied(11));
	assert!(event.is_some());
}

/// The pre-apply hook sees only a retirement that will take effect.
#[test]
fn only_a_new_valid_retirement_reaches_the_pre_apply_hook() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	apply_op(&mut state, 1, first_tier(own, 1), 10);
	let current = TierGeneration::from(&tier(own, 1, "l2://a"));
	let retire = |op_seq, expected| SlabOp {
		op_seq,
		command: SlabCommand::Retire(RetireOp {
			slab: own.slab(SlabId::new(1)),
			expected,
			certified: None,
		}),
	};
	assert!(op_retirement(&state, &retire(2, current.clone()), NODE).is_some());
	assert!(
		op_retirement(&state, &retire(2, current.clone()), 2).is_none(),
		"a wrong leader never reaches the hook"
	);
	let stale = TierGeneration::from(&tier(own, 1, "l2://stale"));
	assert!(
		op_retirement(&state, &retire(2, stale), NODE).is_none(),
		"a rejected retirement never reaches the hook"
	);
	apply_op(&mut state, 2, retire(2, current.clone()).command, 11);
	assert!(
		op_retirement(&state, &retire(2, current.clone()), NODE).is_none(),
		"a replayed receipt never reaches the hook"
	);
	supersede(&mut state, 0xA, 0xB);
	assert!(
		op_retirement(&state, &retire(3, current), NODE).is_none(),
		"a superseded owner never reaches the hook"
	);
}

#[test]
fn a_compaction_that_changes_the_visible_domain_is_a_definite_rejection() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	let mut commit = CompactionCommit::gc_mark_delete_pending(
		own,
		CompactionJobId::new(1),
		crate::schema::ManifestEpoch::new(0),
		vec![SlabId::new(1)],
	);
	commit.digest_summary.visible_post = crate::hash::state_digest::StateDigest::new([9; 32]);
	let (response, event) = apply_op(&mut state, 1, SlabCommand::Compaction(commit), 10);
	assert_eq!(
		outcome(&response),
		&rejected(SlabRejection::VisibleDomainMismatch, 10)
	);
	assert!(event.is_none());
}

#[test]
fn a_snapshot_with_an_invalid_op_table_is_refused() {
	let mut state = registered(0xA);
	state.op_tables.insert(owner(0x77), Default::default());
	assert!(validate_op_tables(&state).is_err(), "an unregistered owner");

	let mut state = registered(0xA);
	let entry = || OpEntry {
		digest: [0; 32],
		outcome: RecordedOutcome::Applied { index: 1 },
		acked: false,
		retirement: None,
	};
	let table = state.op_tables.entry(owner(0xA)).or_default();
	table.acked_through = 5;
	table.outcomes.insert(5, entry());
	assert!(
		validate_op_tables(&state).is_err(),
		"at or below the prefix"
	);

	let table = state.op_tables.entry(owner(0xA)).or_default();
	table.outcomes.clear();
	table.outcomes.insert(5 + OPS_WINDOW_W + 1, entry());
	assert!(validate_op_tables(&state).is_err(), "past the window");

	let table = state.op_tables.entry(owner(0xA)).or_default();
	table.outcomes.clear();
	table.outcomes.insert(5 + OPS_WINDOW_W, entry());
	validate_op_tables(&state).unwrap();
}

/// The digest contract: pinned per kind, so neither a codec change nor a new
/// `SlabCommand` variant can move it silently. ADR 0704 moved Tier and Retire
/// (each carries a trailing `certified` option, encoded even when `None`) with
/// the format bump; the other kinds are unchanged.
#[test]
fn command_digests_are_pinned_per_kind() {
	let own = OwnerRef::for_test(3, 0x0697);
	let hlc = HybridTimestamp {
		physical_ms: 5,
		logical: 6,
		node_id: 3,
	};
	let hex = |command: SlabCommand| {
		command
			.digest()
			.iter()
			.map(|byte| format!("{byte:02x}"))
			.collect::<String>()
	};
	let generation = TierGeneration::from(&tier(own, 7, "l2://a"));
	let commands = [
		(
			"tier",
			SlabCommand::Tier(TierOp {
				tier: tier(own, 7, "l2://b"),
				replaces: Some(generation.clone()),
			}),
		),
		(
			"retire",
			SlabCommand::Retire(RetireOp {
				slab: own.slab(SlabId::new(7)),
				expected: generation,
				certified: None,
			}),
		),
		(
			"compaction",
			SlabCommand::Compaction(CompactionCommit::gc_mark_delete_pending(
				own,
				CompactionJobId::new(9),
				crate::schema::ManifestEpoch::new(4),
				vec![SlabId::new(7)],
			)),
		),
		(
			"gc_remove",
			SlabCommand::GcRemove(GcRemove {
				job_id: CompactionJobId::new(9),
				slab: own.slab(SlabId::new(7)),
				expected_run: RunIdentity {
					compaction_level: CompactionLevelIndex::L1,
					size_bytes: ByteCount::new(4096),
					row_count: RowCount::new(3),
					min_row_hlc: hlc,
					max_row_hlc: hlc,
				},
			}),
		),
		(
			"claim",
			SlabCommand::Claim(ClaimOp {
				slab: own.slab(SlabId::new(7)),
				job_id: CompactionJobId::new(9),
			}),
		),
		(
			"complete",
			SlabCommand::Complete(CompleteOp {
				slab: own.slab(SlabId::new(7)),
				job_id: CompactionJobId::new(9),
			}),
		),
		(
			"burn",
			SlabCommand::BurnReserved(BurnOp {
				slab: own.slab(SlabId::new(7)),
			}),
		),
	];
	let actual: Vec<(&str, String)> = commands
		.into_iter()
		.map(|(name, command)| (name, hex(command)))
		.collect();
	assert_eq!(
		actual,
		vec![
			(
				"tier",
				"71e9ec64eddf825d2bb3b660c83ef74c76a0aea5d89972a4ebe0e3d45cd49ee5".to_string()
			),
			(
				"retire",
				"4e257b4787362ba0977206ee068bbb26ee7fab779383270eb6e7b0e91fa0d501".to_string()
			),
			(
				"compaction",
				"1815cbe5a460b4548b570bffa8453bdbf9e7dcaf159048bddc91cf0714e8526f".to_string()
			),
			(
				"gc_remove",
				"1336b4e81cc6f7c16b6361461c6a15851c454ac6baa0eb1337d51019f2259f98".to_string()
			),
			(
				"claim",
				"332b0c00f4430a896b559ce51953d599819ea35e23d25ce1a9cffbc02a853453".to_string()
			),
			(
				"complete",
				"4a990e72c22018e67e3d975d08548636d5b417c1845ff1183b29a2d66f5e83a4".to_string()
			),
			(
				"burn",
				"e967ccab39fcf6dd2249639ebe583b05f43184486b84fd0645194e10e26dd97c".to_string()
			),
		]
	);
}

// ADR 0697 §4: terminal arbitration at the state machine.

const JOB: CompactionJobId = CompactionJobId::new(0x51);
const OTHER_JOB: CompactionJobId = CompactionJobId::new(0x52);

fn claim_of(owner: OwnerRef, slab: u64, job_id: CompactionJobId) -> SlabCommand {
	SlabCommand::Claim(ClaimOp {
		slab: owner.slab(SlabId::new(slab)),
		job_id,
	})
}

fn complete_of(owner: OwnerRef, slab: u64, job_id: CompactionJobId) -> SlabCommand {
	SlabCommand::Complete(CompleteOp {
		slab: owner.slab(SlabId::new(slab)),
		job_id,
	})
}

fn mark_of(owner: OwnerRef, slab: u64, job_id: CompactionJobId) -> SlabCommand {
	SlabCommand::Compaction(crate::storage::CompactionCommit::gc_mark_delete_pending(
		owner,
		job_id,
		crate::schema::ManifestEpoch::new(4),
		vec![SlabId::new(slab)],
	))
}

fn gc_remove_of(owner: OwnerRef, slab: u64, job_id: CompactionJobId) -> SlabCommand {
	let hlc = HybridTimestamp {
		physical_ms: 1,
		logical: 0,
		node_id: 0,
	};
	SlabCommand::GcRemove(GcRemove {
		job_id,
		slab: owner.slab(SlabId::new(slab)),
		expected_run: RunIdentity {
			compaction_level: CompactionLevelIndex::L1,
			size_bytes: ByteCount::new(4096),
			row_count: RowCount::new(3),
			min_row_hlc: hlc,
			max_row_hlc: hlc,
		},
	})
}

#[track_caller]
fn assert_rejected(response: (WendelonResponse, Option<TierObserverEvent>), reason: SlabRejection) {
	let (response, event) = response;
	match response {
		WendelonResponse::Op(OpResponse::Receipt(OpReceipt {
			outcome: OpOutcome::Recorded(RecordedOutcome::Rejected { reason: actual, .. }),
			..
		})) => assert_eq!(actual, reason),
		other => panic!("expected a rejection, got {other:?}"),
	}
	assert!(event.is_none(), "a rejection notifies no one");
}

#[track_caller]
fn assert_applied(response: (WendelonResponse, Option<TierObserverEvent>)) {
	match response.0 {
		WendelonResponse::Op(OpResponse::Receipt(OpReceipt {
			outcome: OpOutcome::Recorded(RecordedOutcome::Applied { .. }),
			..
		})) => {}
		other => panic!("expected Applied, got {other:?}"),
	}
}

#[test]
fn a_tier_of_a_claimed_or_removed_slab_is_rejected_and_a_claim_of_a_tiered_slab_is_too() {
	let own = owner(1);
	let mut state = registered(1);
	assert_applied(apply_op(&mut state, 1, first_tier(own, 5), 2));
	assert_rejected(
		apply_op(&mut state, 2, claim_of(own, 5, JOB), 3),
		SlabRejection::Tiered,
	);
	assert_rejected(
		apply_op(&mut state, 3, mark_of(own, 5, JOB), 4),
		SlabRejection::Tiered,
	);

	assert_applied(apply_op(&mut state, 4, mark_of(own, 6, JOB), 5));
	assert_rejected(
		apply_op(&mut state, 5, first_tier(own, 6), 6),
		SlabRejection::ClaimedBy { job_id: JOB },
	);
	assert_rejected(
		apply_op(&mut state, 6, claim_of(own, 6, OTHER_JOB), 7),
		SlabRejection::ClaimedBy { job_id: JOB },
	);

	assert_applied(apply_op(&mut state, 7, gc_remove_of(own, 6, JOB), 8));
	assert_rejected(
		apply_op(&mut state, 8, first_tier(own, 6), 9),
		SlabRejection::Removed,
	);
	assert_rejected(
		apply_op(&mut state, 9, claim_of(own, 6, JOB), 10),
		SlabRejection::Removed,
	);
	validate_slab_terminal(&state).unwrap();
}

#[test]
fn a_complete_needs_the_exact_claim_and_moves_it_to_removed() {
	let own = owner(1);
	let mut state = registered(1);
	assert_rejected(
		apply_op(&mut state, 1, complete_of(own, 7, JOB), 2),
		SlabRejection::NotClaimedBy { current: None },
	);
	assert_applied(apply_op(&mut state, 2, claim_of(own, 7, JOB), 3));
	assert_rejected(
		apply_op(&mut state, 3, complete_of(own, 7, OTHER_JOB), 4),
		SlabRejection::NotClaimedBy { current: Some(JOB) },
	);
	assert_rejected(
		apply_op(&mut state, 4, gc_remove_of(own, 7, OTHER_JOB), 5),
		SlabRejection::NotClaimedBy { current: Some(JOB) },
	);
	assert_applied(apply_op(&mut state, 5, complete_of(own, 7, JOB), 6));
	let terminal = &state.slab_terminal[&own];
	assert!(terminal.claimed.is_empty());
	assert!(terminal.removed.contains(SlabId::new(7)));
	assert_rejected(
		apply_op(&mut state, 6, complete_of(own, 7, JOB), 7),
		SlabRejection::Removed,
	);
}

#[test]
fn a_retirement_ends_the_identity_in_its_one_entry() {
	let own = owner(1);
	let mut state = registered(1);
	let tier = tier(own, 5, "l2://a");
	assert_applied(apply_op(
		&mut state,
		1,
		SlabCommand::Tier(TierOp {
			tier: tier.clone(),
			replaces: None,
		}),
		2,
	));
	assert_applied(apply_op(
		&mut state,
		2,
		SlabCommand::Retire(RetireOp {
			slab: tier.slab,
			expected: TierGeneration::from(&tier),
			certified: None,
		}),
		3,
	));
	assert!(state.slab_terminal[&own].removed.contains(SlabId::new(5)));
	assert_rejected(
		apply_op(&mut state, 3, first_tier(own, 5), 4),
		SlabRejection::Removed,
	);
	validate_slab_terminal(&state).unwrap();
}

#[test]
fn a_burn_ends_an_untouched_id_and_nothing_else() {
	let own = owner(1);
	let mut state = registered(1);
	let burn = |slab: u64| {
		SlabCommand::BurnReserved(BurnOp {
			slab: own.slab(SlabId::new(slab)),
		})
	};
	assert_applied(apply_op(&mut state, 1, burn(4), 2));
	assert_rejected(
		apply_op(&mut state, 2, claim_of(own, 4, JOB), 3),
		SlabRejection::Removed,
	);
	assert_rejected(apply_op(&mut state, 3, burn(4), 4), SlabRejection::Removed);
	assert_applied(apply_op(&mut state, 4, first_tier(own, 5), 5));
	assert_rejected(apply_op(&mut state, 5, burn(5), 6), SlabRejection::Tiered);
	assert_applied(apply_op(&mut state, 6, claim_of(own, 6, JOB), 7));
	assert_rejected(
		apply_op(&mut state, 7, burn(6), 8),
		SlabRejection::ClaimedBy { job_id: JOB },
	);
}

#[test]
fn a_new_run_cannot_name_an_ended_identity() {
	let own = owner(1);
	let mut state = registered(1);
	assert_applied(apply_op(&mut state, 1, claim_of(own, 7, JOB), 2));
	assert_applied(apply_op(&mut state, 2, complete_of(own, 7, JOB), 3));
	let hlc = HybridTimestamp {
		physical_ms: 1,
		logical: 0,
		node_id: 0,
	};
	let mut commit = crate::storage::CompactionCommit::gc_mark_delete_pending(
		own,
		OTHER_JOB,
		crate::schema::ManifestEpoch::new(4),
		Vec::new(),
	);
	commit.slabs_to_add.push(crate::storage::LsmRunSummary {
		slab_id: SlabId::new(7),
		compaction_level: CompactionLevelIndex::L1,
		size_bytes: ByteCount::new(1),
		row_count: RowCount::new(1),
		min_row_hlc: hlc,
		max_row_hlc: hlc,
		state: crate::storage::SlabState::Live,
		sidecar_coverage: Default::default(),
		manifest_epoch: crate::schema::ManifestEpoch::new(5),
		retention_debt_score: 0,
		tombstone_debt_score: 0,
		index_coverage_debt_score: 0,
		layout_debt_score: 0,
	});
	assert_rejected(
		apply_op(&mut state, 3, SlabCommand::Compaction(commit), 4),
		SlabRejection::Removed,
	);
}

/// A rejected or superseded claim changes nothing, and a receipt read after
/// supersession is history, not authority.
#[test]
fn the_terminal_sets_are_per_owner_and_history_precedes_current_owner_admission() {
	let (old, new) = (owner(1), owner(2));
	let mut state = registered(1);
	assert_applied(apply_op(&mut state, 1, claim_of(old, 7, JOB), 2));
	supersede(&mut state, 1, 2);
	assert_applied(apply_op(&mut state, 1, first_tier(new, 7), 3));
	let (retry, event) = apply_op(&mut state, 1, claim_of(old, 7, JOB), 4);
	assert!(
		matches!(
			retry,
			WendelonResponse::Op(OpResponse::Receipt(OpReceipt {
				outcome: OpOutcome::Recorded(RecordedOutcome::Applied { .. }),
				..
			}))
		),
		"the old owner's retry still learns its answer: {retry:?}"
	);
	assert!(event.is_none());
	let (fresh, _) = apply_op(&mut state, 2, complete_of(old, 7, JOB), 5);
	assert!(
		matches!(
			fresh,
			WendelonResponse::Refused(SlabRefusal::NotCurrentOwner { .. })
		),
		"a new effect by the old owner is refused: {fresh:?}"
	);
	assert_eq!(state.slab_terminal[&old].claimed[&SlabId::new(7)], JOB);
}

#[test]
fn intervals_coalesce_exactly_and_never_lose_the_top_id() {
	let mut set = crate::consensus::slab_ops::SlabIntervals::default();
	for id in [5, 7, 6, 1, 3, 2, u64::MAX, u64::MAX - 1, 5] {
		set.insert(SlabId::new(id));
	}
	assert_eq!(set.intervals(), &[(1, 3), (5, 7), (u64::MAX - 1, u64::MAX)]);
	assert!(set.is_canonical());
	for id in [1, 3, 5, 7, u64::MAX - 1, u64::MAX] {
		assert!(set.contains(SlabId::new(id)), "{id}");
	}
	for id in [0, 4, 8, u64::MAX - 2] {
		assert!(!set.contains(SlabId::new(id)), "{id}");
	}
}

/// A state that breaks §4 is refused where a snapshot is written, and where one
/// is decoded or installed.
#[test]
fn a_snapshot_that_breaks_the_terminal_sets_is_refused_written_and_read() {
	use crate::consensus::slab_ops::{OwnerTerminal, SlabIntervals};
	let own = owner(1);
	let with = |edit: &dyn Fn(&mut SmState, &mut OwnerTerminal)| {
		let mut state = registered(1);
		let mut terminal = OwnerTerminal::default();
		edit(&mut state, &mut terminal);
		state.slab_terminal.insert(own, terminal);
		state
	};
	let tiered = tier(own, 7, "l2://a");
	let cases: Vec<(&str, SmState, &str)> = vec![
		(
			"claimed and removed",
			with(&|_, terminal| {
				terminal.claimed.insert(SlabId::new(7), JOB);
				terminal.removed.insert(SlabId::new(7));
			}),
			"both claimed and removed",
		),
		(
			"claimed and tiered",
			with(&|state, terminal| {
				terminal.claimed.insert(SlabId::new(7), JOB);
				state.tiered_slabs.insert(tiered.slab, tiered.clone());
			}),
			"both claimed and tiered",
		),
		(
			"removed and tiered",
			with(&|state, terminal| {
				terminal.removed.insert(SlabId::new(7));
				state.tiered_slabs.insert(tiered.slab, tiered.clone());
			}),
			"both tiered and removed",
		),
		(
			"adjacent intervals",
			with(&|_, terminal| {
				terminal.removed = SlabIntervals::from_raw_for_test(vec![(1, 2), (3, 4)]);
			}),
			"not canonical",
		),
		(
			"overlapping intervals",
			with(&|_, terminal| {
				terminal.removed = SlabIntervals::from_raw_for_test(vec![(1, 5), (3, 8)]);
			}),
			"not canonical",
		),
		(
			"inverted interval",
			with(&|_, terminal| {
				terminal.removed = SlabIntervals::from_raw_for_test(vec![(5, 1)]);
			}),
			"not canonical",
		),
		(
			"unregistered owner",
			{
				let mut state = SmState::default();
				state.slab_terminal.insert(own, OwnerTerminal::default());
				state
			},
			"incarnation registry",
		),
	];
	for (name, state, expected) in cases {
		let error = validate_slab_terminal(&state).unwrap_err();
		assert!(error.contains(expected), "{name}: {error}");
		assert!(encode_snapshot(&state).is_err(), "{name}: written");
		let body = postcard::to_stdvec(&state).unwrap();
		let header = SnapshotHeader {
			state_sha256: control_artifact_digest_32(&body),
			..snapshot_header_template(&state)
		};
		let mut bytes = bytemuck::bytes_of(&header).to_vec();
		bytes.extend_from_slice(&body);
		let error = decode_snapshot_reader(bytes.as_slice()).unwrap_err();
		assert!(error.contains(expected), "{name}: read: {error}");
	}
}

/// §9.3: a retry of an accepted operation after what came later changed everything it
/// touched. Its original receipt, at its original index, with no effect.
#[test]
fn an_accepted_retry_after_its_replacement_and_retirement_returns_its_receipt() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	let first = first_tier(own, 1);
	apply_op(&mut state, 1, first.clone(), 10);
	let SlabCommand::Tier(TierOp { tier, .. }) = first.clone() else {
		panic!("a tier");
	};
	let mut newer = tier.clone();
	newer.l2_uri = "memory://replacement".to_string();
	newer.payload_hash = [9; 32];
	let replacement = SlabCommand::Tier(TierOp {
		tier: newer.clone(),
		replaces: Some(TierGeneration::from(&tier)),
	});
	assert_eq!(
		outcome(&apply_op(&mut state, 2, replacement.clone(), 11).0),
		&applied(11)
	);
	let unchanged = state.tiered_slabs.clone();
	let (retry, event) = apply_op(&mut state, 1, first.clone(), 12);
	assert_eq!(outcome(&retry), &applied(10), "after its replacement");
	assert!(event.is_none());
	assert_eq!(state.tiered_slabs, unchanged);

	let retire = SlabCommand::Retire(RetireOp {
		slab: newer.slab,
		expected: TierGeneration::from(&newer),
		certified: None,
	});
	assert_eq!(
		outcome(&apply_op(&mut state, 3, retire.clone(), 13).0),
		&applied(13)
	);
	let unchanged = state.tiered_slabs.clone();
	for (seq, command, index) in [(1, &first, 10), (2, &replacement, 11)] {
		let (retry, event) = apply_op(&mut state, seq, command.clone(), 14);
		assert_eq!(outcome(&retry), &applied(index), "after its retirement");
		assert!(event.is_none());
	}
	assert_eq!(state.tiered_slabs, unchanged);

	let (_, mut restored) = decode_snapshot_reader(&encode_snapshot(&state).unwrap()[..]).unwrap();
	let (retry, event) = apply_op(&mut restored, 1, first, 15);
	assert_eq!(outcome(&retry), &applied(10), "after a snapshot");
	assert!(event.is_none());
}

/// §9.3: a Cancel of another digest, on a key already decided, is an anomaly with no effect.
#[test]
fn a_cancel_of_another_digest_changes_nothing() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	let key = |op_seq| OpKey { owner: own, op_seq };
	let decided = first_tier(own, 1);
	apply_op(&mut state, 1, decided.clone(), 10);
	let before = entries(&state, own);
	let tiers = state.tiered_slabs.clone();
	assert_eq!(
		outcome(&cancel(&mut state, key(1), first_tier(own, 2).digest(), 11)),
		&OpOutcome::Conflict
	);
	assert_eq!(entries(&state, own), before);
	assert_eq!(state.tiered_slabs, tiers);
	let (retry, _) = apply_op(&mut state, 1, decided, 12);
	assert_eq!(outcome(&retry), &applied(10), "its decision stands");
}

// ADR 0704: certified provenance on the ring.

fn generation(owner: OwnerRef, slab: u64, uri: &str) -> TierGeneration {
	TierGeneration::from(&tier(owner, slab, uri))
}

fn certified_tier(
	owner: OwnerRef,
	slab: u64,
	uri: &str,
	op: OpKey,
	source: TierGeneration,
) -> TieredSlab {
	TieredSlab {
		certified: Some(crate::consensus::slab_ops::CertifiedProvenance {
			op,
			source,
			postimage_rows_digest: [0x55; 32],
		}),
		..tier(owner, slab, uri)
	}
}

fn certified_retire(
	owner: OwnerRef,
	slab: u64,
	expected: TierGeneration,
	op: OpKey,
	named_slab: u64,
	source: TierGeneration,
) -> SlabCommand {
	SlabCommand::Retire(RetireOp {
		slab: owner.slab(SlabId::new(slab)),
		expected,
		certified: Some(CertifiedRetirement {
			op,
			slab: owner.slab(SlabId::new(named_slab)),
			source,
		}),
	})
}

fn key(owner: OwnerRef, op_seq: u64) -> OpKey {
	OpKey { owner, op_seq }
}

/// ADR 0704 §2: apply validates certified provenance against its own envelope. A
/// Tier or Retire whose provenance names another operation, another source
/// generation or another slab is rejected, typed, and changes nothing; the same
/// commands with their own provenance apply, and only the applied retirement
/// leaves its link in its own outcome entry.
#[test]
fn apply_refuses_provenance_naming_another_op_source_or_slab() {
	let mut state = registered(0xA);
	let own = owner(0xA);
	assert_applied(apply_op(&mut state, 1, first_tier(own, 1), 10));
	let source = generation(own, 1, "l2://a");
	let other = generation(own, 1, "l2://elsewhere");
	let committed = state.tiered_slabs.clone();
	let tier_with = |op, source, replaces| {
		SlabCommand::Tier(TierOp {
			tier: certified_tier(own, 1, "l2://b", op, source),
			replaces,
		})
	};
	let refused = [
		(
			"tier naming another op",
			2,
			tier_with(key(own, 99), source.clone(), Some(source.clone())),
		),
		(
			"tier naming another owner's op",
			3,
			tier_with(key(owner(0xB), 3), source.clone(), Some(source.clone())),
		),
		(
			"tier naming another source",
			4,
			tier_with(key(own, 4), other.clone(), Some(source.clone())),
		),
		(
			"a certified tier with no replaced generation",
			5,
			SlabCommand::Tier(TierOp {
				tier: certified_tier(own, 2, "l2://b", key(own, 5), source.clone()),
				replaces: None,
			}),
		),
		(
			"retire naming another op",
			6,
			certified_retire(own, 1, source.clone(), key(own, 99), 1, source.clone()),
		),
		(
			"retire naming another source",
			7,
			certified_retire(own, 1, source.clone(), key(own, 7), 1, other.clone()),
		),
		(
			"retire naming another slab",
			8,
			certified_retire(own, 1, source.clone(), key(own, 8), 2, source.clone()),
		),
	];
	for (case, op_seq, command) in refused {
		assert_rejected(
			apply_op(&mut state, op_seq, command, 10 + op_seq),
			SlabRejection::ProvenanceMismatch,
		);
		assert_eq!(state.tiered_slabs, committed, "{case}: nothing applied");
		assert!(
			state
				.slab_terminal
				.get(&own)
				.is_none_or(|terminal| !terminal.removed.contains(SlabId::new(1))),
			"{case}: nothing removed"
		);
		assert!(
			state.op_tables[&own].outcomes[&op_seq].retirement.is_none(),
			"{case}: no retirement link"
		);
	}

	// Their own provenance applies: the committed tier carries it, and only the
	// applied retirement's own entry keeps the link.
	let replacement = certified_tier(own, 1, "l2://b", key(own, 9), source.clone());
	assert_applied(apply_op(
		&mut state,
		9,
		SlabCommand::Tier(TierOp {
			tier: replacement.clone(),
			replaces: Some(source.clone()),
		}),
		20,
	));
	assert_eq!(
		state.tiered_slabs[&own.slab(SlabId::new(1))].certified,
		replacement.certified
	);
	let newer = TierGeneration::from(&replacement);
	assert_applied(apply_op(
		&mut state,
		10,
		certified_retire(own, 1, newer.clone(), key(own, 10), 1, newer.clone()),
		21,
	));
	assert_eq!(
		state.op_tables[&own].outcomes[&10].retirement,
		Some(CertifiedRetirement {
			op: key(own, 10),
			slab: own.slab(SlabId::new(1)),
			source: newer,
		})
	);
	assert!(!state.tiered_slabs.contains_key(&own.slab(SlabId::new(1))));
}

/// ADR 0704 §2: a certified retirement's link lives exactly as long as its own
/// retained outcome. It is found (only for the current owner, only Applied, only
/// its own slab) while retained, survives a snapshot round trip with its entry,
/// and is gone once the acknowledged prefix retires the entry, including after a
/// snapshot. Several matches refuse, typed; a misplaced link fails snapshot
/// validation.
#[test]
fn a_certified_retirements_link_lives_exactly_as_long_as_its_retained_outcome() {
	use super::certified_retirement_in as found;
	let mut state = registered(0xA);
	let own = owner(0xA);
	let slab = own.slab(SlabId::new(1));
	assert_applied(apply_op(&mut state, 1, first_tier(own, 1), 10));
	let source = generation(own, 1, "l2://a");
	assert_applied(apply_op(
		&mut state,
		2,
		certified_retire(own, 1, source.clone(), key(own, 2), 1, source.clone()),
		11,
	));
	// A rejected certified retirement of another slab leaves no link.
	assert_rejected(
		apply_op(
			&mut state,
			3,
			certified_retire(own, 3, source.clone(), key(own, 3), 3, source.clone()),
			12,
		),
		SlabRejection::NotTiered,
	);
	let link = CertifiedRetirement {
		op: key(own, 2),
		slab,
		source,
	};
	assert_eq!(found(&state, own, slab).unwrap(), Some(link.clone()));
	assert_eq!(found(&state, own, own.slab(SlabId::new(3))).unwrap(), None);
	assert_eq!(
		found(&state, owner(0xB), slab).unwrap(),
		None,
		"another owner"
	);

	// Carried with its entry through a snapshot.
	let (_, restored) = decode_snapshot_reader(&encode_snapshot(&state).unwrap()[..]).unwrap();
	assert_eq!(restored.op_tables, state.op_tables);
	assert_eq!(found(&restored, own, slab).unwrap(), Some(link.clone()));

	// A superseded owner's retained link is no authority.
	let mut superseded = state.clone();
	supersede(&mut superseded, 0xA, 0xC);
	assert_eq!(found(&superseded, own, slab).unwrap(), None);

	// Two applied entries naming one slab are inconsistent: refused, never chosen.
	let mut twice = state.clone();
	let mut copy = twice.op_tables[&own].outcomes[&2].clone();
	copy.retirement = Some(CertifiedRetirement {
		op: key(own, 4),
		..link.clone()
	});
	twice
		.op_tables
		.get_mut(&own)
		.unwrap()
		.outcomes
		.insert(4, copy);
	let error = found(&twice, own, slab).unwrap_err();
	assert!(
		error
			.get_ref()
			.is_some_and(|inner| inner.is::<super::InconsistentCertifiedRetirement>()),
		"{error}"
	);

	// A link that is not its own applied entry's fails snapshot validation.
	for (case, retirement, outcome) in [
		(
			"another op's link",
			CertifiedRetirement {
				op: key(own, 9),
				..link.clone()
			},
			RecordedOutcome::Applied { index: 11 },
		),
		(
			"a rejected entry's link",
			link.clone(),
			RecordedOutcome::Rejected {
				reason: SlabRejection::Cancelled,
				index: 11,
			},
		),
	] {
		let mut broken = state.clone();
		let entry = broken
			.op_tables
			.get_mut(&own)
			.unwrap()
			.outcomes
			.get_mut(&2)
			.unwrap();
		entry.retirement = Some(retirement);
		entry.outcome = outcome;
		assert!(validate_op_tables(&broken).is_err(), "{case}");
	}

	// The acknowledged prefix retires the entry, and its link with it.
	ack(&mut state, own, vec![1, 2, 3]);
	assert_eq!(state.op_tables[&own].acked_through, 3);
	assert_eq!(found(&state, own, slab).unwrap(), None);
	let (_, restored) = decode_snapshot_reader(&encode_snapshot(&state).unwrap()[..]).unwrap();
	assert_eq!(found(&restored, own, slab).unwrap(), None);
	assert!(restored.op_tables[&own].outcomes.is_empty());
}

/// ADR 0704 §2: provenance is not identity. `TierGeneration` and the store's own
/// pointer are the same with and without it.
#[test]
fn provenance_leaves_the_tier_generation_unchanged() {
	let own = owner(0xA);
	let plain = tier(own, 1, "l2://b");
	let certified = certified_tier(own, 1, "l2://b", key(own, 2), generation(own, 1, "l2://a"));
	assert_ne!(plain, certified);
	assert_eq!(
		TierGeneration::from(&plain),
		TierGeneration::from(&certified)
	);
	assert_eq!(
		crate::storage::TieredSlabPointer::from_ring(&plain),
		crate::storage::TieredSlabPointer::from_ring(&certified)
	);
}

/// ADR 0704 §2: in ParentDecisions mode (the state carries decisions) a
/// retirement without certified provenance is rejected, typed, and retires
/// nothing; a certified one applies. Today keeps the ordinary retirement.
#[test]
fn in_parent_decisions_mode_only_a_certified_retirement_applies() {
	let own = owner(0xA);
	let source = generation(own, 1, "l2://a");
	let ordinary = SlabCommand::Retire(RetireOp {
		slab: own.slab(SlabId::new(1)),
		expected: source.clone(),
		certified: None,
	});

	// ADR 0725: in this mode a slab operation is its owner's by signature, so
	// the owner binds its key and each operation is applied signed, appended by
	// the owner's own node as before.
	let signing = ed25519_dalek::SigningKey::from_bytes(&[0x0A; 32]);
	let mut mode = registered(0xA);
	mode.decisions = Some(Default::default());
	let bound = apply_request_at_log_index(
		&mut mode,
		WendelonRequest::Decision(crate::consensus::decisions::DecisionCommand::BindSigner {
			signer: crate::consensus::decisions::Signer {
				node: NODE,
				incarnation: own.incarnation(),
			},
			key: signing.verifying_key().to_bytes(),
		}),
		9,
		NODE,
	)
	.0;
	assert_eq!(
		bound,
		WendelonResponse::Decision(crate::consensus::decisions::DecisionResult::SignerBound)
	);
	let signed = |state: &mut SmState, op_seq: u64, command: SlabCommand, index: u64| {
		apply_request_at_log_index(
			state,
			WendelonRequest::OwnerOp(crate::consensus::owner_ops::SignedOwnerOp::submit_for_test(
				&signing,
				SlabOp { op_seq, command },
			)),
			index,
			NODE,
		)
	};
	assert_applied(signed(&mut mode, 1, first_tier(own, 1), 10));
	assert_rejected(
		signed(&mut mode, 2, ordinary.clone(), 11),
		SlabRejection::CertificationRequired,
	);
	assert!(mode.tiered_slabs.contains_key(&own.slab(SlabId::new(1))));
	// The same certified retirement, bare, is refused before it is looked at.
	let certified = certified_retire(own, 1, source.clone(), key(own, 3), 1, source);
	assert_eq!(
		apply_op(&mut mode, 3, certified.clone(), 12).0,
		WendelonResponse::Refused(SlabRefusal::OwnerOpUnsigned)
	);
	assert!(mode.tiered_slabs.contains_key(&own.slab(SlabId::new(1))));
	assert_applied(signed(&mut mode, 3, certified, 12));
	assert!(!mode.tiered_slabs.contains_key(&own.slab(SlabId::new(1))));

	let mut today = registered(0xA);
	assert_applied(apply_op(&mut today, 1, first_tier(own, 1), 10));
	assert_applied(apply_op(&mut today, 2, ordinary, 11));
	assert!(!today.tiered_slabs.contains_key(&own.slab(SlabId::new(1))));
}

/// ADR 0704 §2, through a state machine and its canonical lease: the lease reads
/// the committed generation's provenance, and the certified retirement link while
/// its outcome is retained; once the acknowledged prefix retires the outcome, the
/// lease finds no link.
#[tokio::test]
async fn the_lease_reads_committed_provenance_and_a_retained_retirement_link() {
	use openraft::storage::RaftStateMachine;
	use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
	let own = owner(0xA);
	let slab = own.slab(SlabId::new(1));
	let source = generation(own, 1, "l2://a");
	let replacement = certified_tier(own, 1, "l2://b", key(own, 2), source.clone());
	let newer = TierGeneration::from(&replacement);
	let entry = |index: u64, request: WendelonRequest| Entry::<WendelonTypeConfig> {
		log_id: LogId::new(CommittedLeaderId::new(1, NODE), index),
		payload: EntryPayload::Normal(request),
	};
	let op = |op_seq, command| WendelonRequest::SlabOp(SlabOp { op_seq, command });
	let mut sm = WendelonStateMachine::new();
	let source_of_lease = sm.tier_snapshot_source_for_test();
	RaftStateMachine::apply(
		&mut sm,
		[
			entry(
				1,
				WendelonRequest::RegisterIncarnation(RegisterIncarnation {
					node: NODE,
					incarnation: own.incarnation(),
					supersedes: None,
				}),
			),
			entry(2, op(1, first_tier(own, 1))),
			entry(
				3,
				op(
					2,
					SlabCommand::Tier(TierOp {
						tier: replacement.clone(),
						replaces: Some(source),
					}),
				),
			),
		],
	)
	.await
	.unwrap();
	{
		let lease = source_of_lease.read().await.unwrap();
		assert_eq!(lease.committed_provenance(slab), replacement.certified);
		assert_eq!(lease.certified_retirement(own, slab).unwrap(), None);
	}
	RaftStateMachine::apply(
		&mut sm,
		[entry(
			4,
			op(
				3,
				certified_retire(own, 1, newer.clone(), key(own, 3), 1, newer.clone()),
			),
		)],
	)
	.await
	.unwrap();
	{
		let lease = source_of_lease.read().await.unwrap();
		assert_eq!(lease.committed_provenance(slab), None, "retired");
		assert_eq!(
			lease.certified_retirement(own, slab).unwrap(),
			Some(CertifiedRetirement {
				op: key(own, 3),
				slab,
				source: newer,
			})
		);
	}
	RaftStateMachine::apply(
		&mut sm,
		[entry(
			5,
			WendelonRequest::AckOutcomes(AckOutcomes {
				owner: own,
				seqs: vec![1, 2, 3],
			}),
		)],
	)
	.await
	.unwrap();
	let lease = source_of_lease.read().await.unwrap();
	assert_eq!(
		lease.certified_retirement(own, slab).unwrap(),
		None,
		"pruned with its outcome"
	);
}
