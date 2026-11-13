//! ADR 0725: owner-signed slab operations at apply. A `ParentDecisions` state
//! machine takes a slab operation only with its owner's signature, admits a
//! follower's first publication, cancel and acknowledgement from any appending
//! leader, and keeps the appending-leader rule for every other command.

use super::*;
use crate::consensus::decisions::tests as fixture;
use crate::consensus::owner_ops::SignedOwnerOp;
use crate::consensus::slab_ops::{
	AckOutcomes, CancelOp, CertifiedRetirement, ClaimOp, OpKey, OpOutcome, OpReceipt, OpResponse,
	RecordedOutcome, RetireOp, SlabCommand, SlabOp, SlabRejection, TierGeneration, TierOp,
};
use crate::schema::{CompactionJobId, SlabId};

const LEADER: u32 = 1;
const FOLLOWER: u32 = 2;

fn owner(node: u32) -> OwnerRef {
	let signer = fixture::signer(node);
	OwnerRef::new(signer.node, signer.incarnation)
}

fn pointer(owner: OwnerRef, slab: u64, uri: &str) -> TieredSlab {
	TieredSlab {
		slab: owner.slab(SlabId::new(slab)),
		l2_uri: uri.to_string(),
		payload_hash: [slab as u8; 32],
		is_encrypted: false,
		tenant_id: None,
		tenant_kek_id: None,
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: crate::schema::TombstoneMetadata::default(),
		supersession_metadata: crate::schema::SupersessionMetadata::default(),
		certified: None,
	}
}

fn first_publication(owner: OwnerRef, op_seq: u64, slab: u64) -> SlabOp {
	SlabOp {
		op_seq,
		command: SlabCommand::Tier(TierOp {
			tier: pointer(owner, slab, &format!("l2://{}/slab/{slab}", owner.node())),
			replaces: None,
		}),
	}
}

fn signed(node: u32, op: SlabOp) -> WendelonRequest {
	WendelonRequest::OwnerOp(SignedOwnerOp::submit_for_test(&fixture::test_key(node), op))
}

fn apply(state: &mut SmState, request: WendelonRequest, leader: u32) -> WendelonResponse {
	apply_at_leader_for_test(state, request, fixture::peek_index() + 100, leader)
}

fn recorded(response: &WendelonResponse) -> RecordedOutcome {
	match response {
		WendelonResponse::Op(OpResponse::Receipt(OpReceipt {
			outcome: OpOutcome::Recorded(outcome),
			..
		})) => outcome.clone(),
		other => panic!("expected a recorded outcome, got {other:?}"),
	}
}

fn is_applied(response: &WendelonResponse) -> bool {
	matches!(recorded(response), RecordedOutcome::Applied { .. })
}

/// The whole replicated state a slab operation can change.
fn slab_state(state: &SmState) -> impl PartialEq + std::fmt::Debug + use<> {
	(
		state.tiered_slabs.clone(),
		state.op_tables.clone(),
		state.slab_terminal.clone(),
	)
}

#[test]
fn a_followers_signed_first_publication_applies_under_another_leader() {
	let mut state = fixture::base(3);
	let op = first_publication(owner(FOLLOWER), 1, 7);
	let SlabCommand::Tier(TierOp { tier, .. }) = op.command.clone() else {
		unreachable!()
	};
	let response = apply(&mut state, signed(FOLLOWER, op.clone()), LEADER);
	assert!(is_applied(&response), "{response:?}");
	assert_eq!(state.tiered_slabs.get(&tier.slab), Some(&tier));
	// The owner's own node as leader is admitted as before.
	let own = first_publication(owner(LEADER), 1, 7);
	assert!(is_applied(&apply(&mut state, signed(LEADER, own), LEADER)));
	// Two owners, one local id: two pointers.
	assert_eq!(state.tiered_slabs.len(), 2);
}

#[test]
fn a_bare_slab_operation_is_refused_unsigned_whoever_appended_it() {
	let mut state = fixture::base(3);
	let before = slab_state(&state);
	let op = first_publication(owner(LEADER), 1, 7);
	let key = OpKey {
		owner: owner(LEADER),
		op_seq: 1,
	};
	for request in [
		WendelonRequest::SlabOp(op.clone()),
		WendelonRequest::CancelOp(CancelOp {
			key,
			target_digest: op.command.digest(),
		}),
		WendelonRequest::AckOutcomes(AckOutcomes {
			owner: owner(LEADER),
			seqs: vec![1],
		}),
	] {
		// Appended by the owner's own node as leader: today's rule would admit it.
		assert_eq!(
			apply(&mut state, request, LEADER),
			WendelonResponse::Refused(SlabRefusal::OwnerOpUnsigned)
		);
	}
	assert_eq!(slab_state(&state), before);
}

#[test]
fn todays_authority_refuses_an_owner_operation_and_keeps_its_bare_ones() {
	let mut state = fixture::base(3);
	state.decisions = None;
	let op = first_publication(owner(LEADER), 1, 7);
	assert_eq!(
		apply(&mut state, signed(LEADER, op.clone()), LEADER),
		WendelonResponse::Refused(SlabRefusal::OwnerOpNotInMode)
	);
	assert!(state.tiered_slabs.is_empty());
	// Today's bare operation: admitted from its owner's leader entry only.
	assert_eq!(
		apply(&mut state, WendelonRequest::SlabOp(op.clone()), FOLLOWER),
		WendelonResponse::Refused(SlabRefusal::NotAppendingLeader {
			owner_node: LEADER,
			leader_node: FOLLOWER,
		})
	);
	assert!(is_applied(&apply(
		&mut state,
		WendelonRequest::SlabOp(op),
		LEADER
	)));
}

#[test]
fn a_signature_by_another_key_or_signer_is_refused_before_any_effect() {
	let mut state = fixture::base(3);
	let before = slab_state(&state);
	let op = first_publication(owner(FOLLOWER), 1, 7);
	// Node 3's key over node 2's operation.
	let forged = WendelonRequest::OwnerOp(SignedOwnerOp::submit_for_test(
		&fixture::test_key(3),
		op.clone(),
	));
	assert_eq!(
		apply(&mut state, forged, LEADER),
		WendelonResponse::Refused(SlabRefusal::OwnerOpUnverifiable {
			owner: owner(FOLLOWER)
		})
	);
	// Node 3 signs honestly as itself over node 2's operation.
	let other = SignedOwnerOp::submit_for_test(&fixture::test_key(3), op.clone())
		.attributed_to_for_test(fixture::signer(3));
	assert_eq!(
		apply(&mut state, WendelonRequest::OwnerOp(other), LEADER),
		WendelonResponse::Refused(SlabRefusal::OwnerOpSignerMismatch {
			owner: owner(FOLLOWER)
		})
	);
	// An incarnation with no bound key verifies nothing.
	let unbound = OwnerRef::for_test(FOLLOWER, 0xdead);
	let stranger = first_publication(unbound, 1, 7);
	assert_eq!(
		apply(&mut state, signed(FOLLOWER, stranger), LEADER),
		WendelonResponse::Refused(SlabRefusal::OwnerOpUnverifiable { owner: unbound })
	);
	assert_eq!(slab_state(&state), before);
}

#[test]
fn a_submit_signature_does_not_cancel_and_an_ack_signature_covers_its_batch() {
	let mut state = fixture::base(3);
	let before = slab_state(&state);
	let op = first_publication(owner(FOLLOWER), 1, 7);
	let submit = SignedOwnerOp::submit_for_test(&fixture::test_key(FOLLOWER), op.clone());
	let as_cancel = submit.submit_signature_as_cancel_for_test().unwrap();
	assert_eq!(
		apply(&mut state, WendelonRequest::OwnerOp(as_cancel), LEADER),
		WendelonResponse::Refused(SlabRefusal::OwnerOpUnverifiable {
			owner: owner(FOLLOWER)
		})
	);
	assert_eq!(slab_state(&state), before, "nothing was cancelled");
	// The operation itself still applies: no Cancel was recorded against it.
	assert!(is_applied(&apply(
		&mut state,
		WendelonRequest::OwnerOp(submit),
		LEADER
	)));
	assert!(is_applied(&apply(
		&mut state,
		signed(FOLLOWER, first_publication(owner(FOLLOWER), 2, 8)),
		LEADER
	)));
	let ack = SignedOwnerOp::ack(&fixture::test_key(FOLLOWER), owner(FOLLOWER), vec![1]);
	let widened = ack.ack_with_added_seq_for_test(2).unwrap();
	let before = slab_state(&state);
	assert_eq!(
		apply(&mut state, WendelonRequest::OwnerOp(widened), LEADER),
		WendelonResponse::Refused(SlabRefusal::OwnerOpUnverifiable {
			owner: owner(FOLLOWER)
		})
	);
	assert_eq!(slab_state(&state), before, "nothing was acknowledged");
	// The signed batch acknowledges exactly itself, from a follower.
	match apply(&mut state, WendelonRequest::OwnerOp(ack), LEADER) {
		WendelonResponse::Op(OpResponse::Ack(receipt)) => {
			assert_eq!(receipt.accepted, vec![1]);
			assert_eq!(receipt.acked_through, 1);
		}
		other => panic!("{other:?}"),
	}
}

/// A certified retirement of `tier` as operation `op_seq` of its owner.
fn certified_retire(tier: &TieredSlab, op_seq: u64) -> SlabOp {
	let key = OpKey {
		owner: tier.slab.owner(),
		op_seq,
	};
	SlabOp {
		op_seq,
		command: SlabCommand::Retire(RetireOp {
			slab: tier.slab,
			expected: TierGeneration::from(tier),
			certified: Some(CertifiedRetirement {
				op: key,
				slab: tier.slab,
				source: TierGeneration::from(tier),
			}),
		}),
	}
}

#[test]
fn every_other_command_still_needs_its_owner_to_be_the_appending_leader() {
	let mut state = fixture::base(3);
	let follower = owner(FOLLOWER);
	let first = first_publication(follower, 1, 7);
	let SlabCommand::Tier(TierOp { tier, .. }) = first.command.clone() else {
		unreachable!()
	};
	assert!(is_applied(&apply(
		&mut state,
		signed(FOLLOWER, first),
		LEADER
	)));
	let replacing = SlabOp {
		op_seq: 2,
		command: SlabCommand::Tier(TierOp {
			tier: pointer(follower, 7, "l2://2/slab/7/rewritten"),
			replaces: Some(TierGeneration::from(&tier)),
		}),
	};
	let claim = SlabOp {
		op_seq: 2,
		command: SlabCommand::Claim(ClaimOp {
			slab: follower.slab(SlabId::new(9)),
			job_id: CompactionJobId::new(5),
		}),
	};
	let forbidden = [replacing, certified_retire(&tier, 2), claim];
	let before = slab_state(&state);
	for op in &forbidden {
		assert_eq!(
			apply(&mut state, signed(FOLLOWER, op.clone()), LEADER),
			WendelonResponse::Refused(SlabRefusal::NotAppendingLeader {
				owner_node: FOLLOWER,
				leader_node: LEADER,
			}),
			"{op:?}"
		);
		// The pre-apply hook sees no retirement in a refused one.
		assert!(admitted_retirement(&state, &signed(FOLLOWER, op.clone()), LEADER).is_none());
	}
	assert_eq!(slab_state(&state), before);
	// Appended by its owner's own node as leader, the same signed retirement
	// is admitted, and the pre-apply hook sees exactly it.
	let retire = signed(FOLLOWER, certified_retire(&tier, 2));
	assert_eq!(
		admitted_retirement(&state, &retire, FOLLOWER).map(|(slab, _)| slab),
		Some(tier.slab)
	);
	// A bare retirement retires nothing here, even from its owner's leader entry.
	assert!(
		admitted_retirement(
			&state,
			&WendelonRequest::SlabOp(certified_retire(&tier, 2)),
			FOLLOWER
		)
		.is_none()
	);
	assert!(is_applied(&apply(&mut state, retire, FOLLOWER)));
	assert!(!state.tiered_slabs.contains_key(&tier.slab));
}

#[test]
fn a_signed_retry_learns_its_recorded_outcome_and_is_never_a_new_effect() {
	let mut state = fixture::base(3);
	let op = first_publication(owner(FOLLOWER), 1, 7);
	let first = apply(&mut state, signed(FOLLOWER, op.clone()), LEADER);
	assert!(is_applied(&first));
	let after = slab_state(&state);
	// The retry lands under another leader and answers the same record.
	let retry = apply(&mut state, signed(FOLLOWER, op.clone()), 3);
	assert_eq!(recorded(&retry), recorded(&first));
	assert_eq!(slab_state(&state), after);
	// Another command under the same sequence is a conflict, not an effect.
	let other = first_publication(owner(FOLLOWER), 1, 8);
	match apply(&mut state, signed(FOLLOWER, other), LEADER) {
		WendelonResponse::Op(OpResponse::Receipt(receipt)) => {
			assert_eq!(receipt.outcome, OpOutcome::Conflict)
		}
		other => panic!("{other:?}"),
	}
	assert_eq!(slab_state(&state), after);
}

#[test]
fn a_follower_cancels_its_own_undecided_operation() {
	let mut state = fixture::base(3);
	let op = first_publication(owner(FOLLOWER), 1, 7);
	let cancel = CancelOp {
		key: OpKey {
			owner: owner(FOLLOWER),
			op_seq: 1,
		},
		target_digest: op.command.digest(),
	};
	let response = apply(
		&mut state,
		WendelonRequest::OwnerOp(SignedOwnerOp::cancel_for_test(
			&fixture::test_key(FOLLOWER),
			cancel,
		)),
		LEADER,
	);
	assert!(matches!(
		recorded(&response),
		RecordedOutcome::Rejected {
			reason: SlabRejection::Cancelled,
			..
		}
	));
	// The cancelled operation, arriving late, learns it was cancelled.
	let late = apply(&mut state, signed(FOLLOWER, op), LEADER);
	assert_eq!(recorded(&late), recorded(&response));
	assert!(state.tiered_slabs.is_empty());
}

/// A follower's window advances by its own acknowledgements: past W decided and
/// unacknowledged operations the next is refused, and once it acknowledges, as
/// a follower, more than W operations have been admitted in all.
#[test]
fn a_followers_window_advances_past_w_by_its_own_acknowledgements() {
	use crate::consensus::slab_ops::OPS_WINDOW_W;
	let mut state = fixture::base(3);
	let follower = owner(FOLLOWER);
	for seq in 1..=OPS_WINDOW_W {
		let op = first_publication(follower, seq, seq);
		assert!(is_applied(&apply(&mut state, signed(FOLLOWER, op), LEADER)));
	}
	let beyond = first_publication(follower, OPS_WINDOW_W + 1, OPS_WINDOW_W + 1);
	assert_eq!(
		apply(&mut state, signed(FOLLOWER, beyond.clone()), LEADER),
		WendelonResponse::Refused(SlabRefusal::OutsideWindow {
			key: OpKey {
				owner: follower,
				op_seq: OPS_WINDOW_W + 1,
			},
			acked_through: 0,
		})
	);
	let ack = SignedOwnerOp::ack(&fixture::test_key(FOLLOWER), follower, vec![1, 2, 3]);
	match apply(&mut state, WendelonRequest::OwnerOp(ack), LEADER) {
		WendelonResponse::Op(OpResponse::Ack(receipt)) => assert_eq!(receipt.acked_through, 3),
		other => panic!("{other:?}"),
	}
	for seq in OPS_WINDOW_W + 1..=OPS_WINDOW_W + 3 {
		let op = first_publication(follower, seq, seq);
		assert!(
			is_applied(&apply(&mut state, signed(FOLLOWER, op), LEADER)),
			"operation {seq}"
		);
	}
	// The window is the owner's own: an unacknowledged earlier operation still
	// bounds it.
	let next = first_publication(follower, OPS_WINDOW_W + 4, OPS_WINDOW_W + 4);
	assert!(matches!(
		apply(&mut state, signed(FOLLOWER, next), LEADER),
		WendelonResponse::Refused(SlabRefusal::OutsideWindow { .. })
	));
	assert_eq!(state.tiered_slabs.len() as u64, OPS_WINDOW_W + 3);
}
