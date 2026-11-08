//! ADR 0699 S3a: what the leader admits from a forwarded request.

use super::*;
use crate::consensus::decisions::tests as fixture;
use crate::consensus::monitor::FakeClock;
use crate::consensus::raft_storage::apply_at_leader_for_test;

fn trusted() -> BTreeMap<u32, [u8; 32]> {
	(1..=3)
		.map(|id| {
			(
				id,
				crate::consensus::decisions::decision_verifying_key_bytes(
					crate::consensus::decisions::dev_decision_signing_secret(id),
				),
			)
		})
		.collect()
}

fn healthy() -> Result<u64, ProposalRefusal> {
	Ok(fixture::T0)
}

fn reserve_request(state: &SmState) -> (ForwardedDecision, u32) {
	let claim = fixture::claim(fixture::key(fixture::T0, 1), fixture::children(state, 1), 0);
	let holder = claim.children[0].holder;
	(
		ForwardedDecision::Reserve {
			claim: fixture::sign_for_test(claim, holder),
		},
		holder.node,
	)
}

#[test]
fn today_admits_nothing() {
	let mode = DecisionMode::Today;
	let request = ForwardedDecision::BindSigner {
		incarnation: fixture::inc(1),
	};
	assert_eq!(
		admit(mode, 1, request, &trusted(), healthy),
		Err(ProposalRefusal::ModeToday)
	);
}

#[test]
fn the_leader_stamps_a_reserve_and_only_for_its_coordinator() {
	let mode = DecisionMode::ParentDecisions;
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	let (request, coordinator) = reserve_request(&state);
	let other = coordinator % 3 + 1;
	assert_eq!(
		admit(mode, other, request.clone(), &trusted(), healthy),
		Err(ProposalRefusal::NotTheSender)
	);
	assert_eq!(
		admit(mode, coordinator, request.clone(), &trusted(), || Err(
			ProposalRefusal::ClockUnhealthy
		)),
		Err(ProposalRefusal::ClockUnhealthy)
	);
	let Ok(WendelonRequest::Decision(DecisionCommand::Reserve { stamp_ms, .. })) =
		admit(mode, coordinator, request, &trusted(), healthy)
	else {
		panic!("a Reserve");
	};
	assert_eq!(stamp_ms, fixture::T0);
}

#[test]
fn a_binding_names_only_the_sender_and_its_configured_key() {
	let mode = DecisionMode::ParentDecisions;
	let request = ForwardedDecision::BindSigner {
		incarnation: fixture::inc(2),
	};
	let Ok(WendelonRequest::Decision(DecisionCommand::BindSigner { signer, key })) =
		admit(mode, 2, request.clone(), &trusted(), || {
			panic!("a binding reads no clock")
		})
	else {
		panic!("a binding");
	};
	assert_eq!((signer.node, key), (2, trusted()[&2]));
	assert_eq!(
		admit(mode, 9, request, &trusted(), healthy),
		Err(ProposalRefusal::UnknownSignerKey)
	);
	let Ok(WendelonRequest::RegisterIncarnation(register)) = admit(
		mode,
		3,
		ForwardedDecision::RegisterIncarnation {
			incarnation: fixture::inc(3),
		},
		&trusted(),
		healthy,
	) else {
		panic!("a registration");
	};
	assert_eq!(register.node, 3);
}

#[test]
fn a_follower_registers_once_through_the_leader_and_is_never_replaced() {
	let mode = DecisionMode::ParentDecisions;
	let mut state = fixture::base(3);
	state.incarnations.remove(&2);
	let register = |incarnation| {
		admit(
			mode,
			2,
			ForwardedDecision::RegisterIncarnation { incarnation },
			&trusted(),
			healthy,
		)
		.unwrap()
	};
	// First registration, proposed by leader 1 for authenticated node 2, and
	// its exact retry.
	for index in [1, 2] {
		assert_eq!(
			apply_at_leader_for_test(&mut state, register(fixture::inc(2)), index, 1),
			WendelonResponse::Success
		);
	}
	assert_eq!(state.incarnations[&2].current, Some(fixture::inc(2)));
	// Another store under the same node id cannot retire it.
	let other = StoreIncarnation::from_raw(8_000);
	assert!(matches!(
		apply_at_leader_for_test(&mut state, register(other), 3, 1),
		WendelonResponse::Refused(_)
	));
	assert_eq!(state.incarnations[&2].current, Some(fixture::inc(2)));
	// Binding follows the registered incarnation only.
	let bind = |incarnation| {
		admit(
			mode,
			2,
			ForwardedDecision::BindSigner { incarnation },
			&trusted(),
			healthy,
		)
		.unwrap()
	};
	assert_eq!(
		apply_at_leader_for_test(&mut state, bind(fixture::inc(2)), 4, 1),
		WendelonResponse::Decision(DecisionResult::SignerBound)
	);
	assert_eq!(
		apply_at_leader_for_test(&mut state, bind(other), 5, 1),
		WendelonResponse::Decision(DecisionResult::Refused(
			crate::consensus::decisions::DecisionRefusal::SignerNotCurrent
		))
	);
}

#[test]
fn maintenance_registers_binds_or_leaves_another_store_alone() {
	let mine = fixture::inc(2);
	let other = StoreIncarnation::from_raw(8_000);
	assert_eq!(
		maintenance_step(None, false, mine),
		Some(ForwardedDecision::RegisterIncarnation { incarnation: mine })
	);
	assert_eq!(
		maintenance_step(Some(mine), false, mine),
		Some(ForwardedDecision::BindSigner { incarnation: mine })
	);
	assert_eq!(maintenance_step(Some(mine), true, mine), None);
	// A different established store at this node id is never retired from here.
	assert_eq!(maintenance_step(Some(other), false, mine), None);
	assert_eq!(maintenance_step(Some(other), true, mine), None);
}

#[test]
fn today_keeps_the_appending_leader_rule_for_registration() {
	let register = || {
		WendelonRequest::RegisterIncarnation(RegisterIncarnation {
			node: 2,
			incarnation: fixture::inc(2),
			supersedes: None,
		})
	};
	// Appended by leader 1, not by node 2.
	assert!(matches!(
		apply_at_leader_for_test(&mut SmState::default(), register(), 1, 1),
		WendelonResponse::Refused(_)
	));
	let mut mode_state = fixture::base(3);
	mode_state.incarnations.remove(&2);
	assert_eq!(
		apply_at_leader_for_test(&mut mode_state, register(), 1, 1),
		WendelonResponse::Success
	);
}

#[test]
fn the_leader_clock_must_pass_the_drift_test_at_proposal() {
	let clock = Arc::new(FakeClock::new(fixture::T0));
	assert_eq!(
		leader_clock_now(None, &*clock),
		Err(ProposalRefusal::ClockUnhealthy)
	);
	let tracker = NodeTracker::new(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		clock.clone(),
	);
	assert_eq!(
		leader_clock_now(Some(&tracker), &*clock),
		Err(ProposalRefusal::ClockUnhealthy),
		"no cluster median yet"
	);
	for peer in [2, 3] {
		tracker.record(
			NodeId {
				id: peer,
				_reserved: 0,
			},
			fixture::T0,
		);
	}
	assert_eq!(leader_clock_now(Some(&tracker), &*clock), Ok(fixture::T0));
	// The peers report times further behind than the drift bound allows.
	for peer in [2, 3] {
		tracker.record(
			NodeId {
				id: peer,
				_reserved: 0,
			},
			fixture::T0 - MAX_OFFSET_MS - 1,
		);
	}
	assert_eq!(
		leader_clock_now(Some(&tracker), &*clock),
		Err(ProposalRefusal::ClockUnhealthy)
	);
}

#[test]
fn the_floor_proposal_follows_the_healthy_clock_and_never_lowers() {
	let state = fixture::base(3);
	assert_eq!(
		floor_proposal(&state, DECISION_EXPIRY_MS - 1),
		None,
		"underflow"
	);
	assert_eq!(
		floor_proposal(&state, DECISION_EXPIRY_MS),
		None,
		"floor 0 is not higher"
	);
	let now = fixture::T0 + DECISION_EXPIRY_MS;
	assert_eq!(
		floor_proposal(&state, now),
		Some(WendelonRequest::Decision(DecisionCommand::AdvanceFloor {
			floor_ms: fixture::T0,
			stamp_ms: now,
		}))
	);
	assert_eq!(floor_proposal(&SmState::default(), now), None, "today");
}

#[test]
fn an_oversized_or_malformed_frame_is_refused_before_decode() {
	let oversized = vec![0u8; MAX_DECISION_FRAME_BYTES + 1];
	assert!(decode_bounded::<ForwardedDecision>(&oversized).is_err());
	assert!(decode_bounded::<ForwardedDecision>(&[0xFF, 0xFF, 0xFF]).is_err());
	let request = ForwardedDecision::BindSigner {
		incarnation: fixture::inc(1),
	};
	let bytes = postcard::to_stdvec(&request).unwrap();
	assert_eq!(
		decode_bounded::<ForwardedDecision>(&bytes).unwrap(),
		request
	);
}
