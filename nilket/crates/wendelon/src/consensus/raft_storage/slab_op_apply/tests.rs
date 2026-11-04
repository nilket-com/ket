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
/// `SlabCommand` variant can move it silently.
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
				owner: own,
				job_id: CompactionJobId::new(9),
				slab: SlabId::new(7),
				expected_run: RunIdentity {
					compaction_level: CompactionLevelIndex::L1,
					size_bytes: ByteCount::new(4096),
					row_count: RowCount::new(3),
					min_row_hlc: hlc,
					max_row_hlc: hlc,
				},
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
				"66e90e334a75580067c399f3518d182576560f68b7acf6058eac9979a7f87ca9".to_string()
			),
			(
				"retire",
				"5d3e1b0103831f35d88502702206807b42b62c3f779e41fc654bd8e5557c0b4f".to_string()
			),
			(
				"compaction",
				"1815cbe5a460b4548b570bffa8453bdbf9e7dcaf159048bddc91cf0714e8526f".to_string()
			),
			(
				"gc_remove",
				"b3aab68dd064e8425a687a5f876cc65b2dd42f80ab443635db02da864d6b5ba1".to_string()
			),
		]
	);
}
