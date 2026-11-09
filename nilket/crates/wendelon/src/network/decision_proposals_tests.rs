//! ADR 0699 S3a: what the leader admits from a forwarded request.

use super::*;
use crate::consensus::decisions::tests as fixture;
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

/// ADR 0700 (r1 R1): exactly Reserve and RecordNoop bear time. The selection
/// `propose_as` uses leaves both unstamped, so they reach Raft only stamped by
/// the gated clock and both refuse unhealthy time; the operations that do not
/// need time stay usable while it is unhealthy.
#[test]
fn only_reserve_and_record_noop_bear_time_and_both_need_the_gate() {
	let mode = DecisionMode::ParentDecisions;
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	let (reserve, coordinator) = reserve_request(&state);
	let body = fixture::noop_body(&state, 2, 1);
	let noop_sender = body.coordinator.node;
	let noop = ForwardedDecision::RecordNoop {
		noop: fixture::sign_for_test(body.clone(), body.coordinator),
	};
	let unhealthy = || Err(ProposalRefusal::ClockUnhealthy);
	assert!(matches!(
		admission(mode, coordinator, reserve.clone(), &trusted()),
		Ok(Admitted::Unstamped(Unstamped::Reserve { .. }))
	));
	assert!(matches!(
		admission(mode, noop_sender, noop.clone(), &trusted()),
		Ok(Admitted::Unstamped(Unstamped::RecordNoop { .. }))
	));
	for (sender, request) in [(coordinator, reserve), (noop_sender, noop.clone())] {
		assert_eq!(
			admit(mode, sender, request, &trusted(), unhealthy),
			Err(ProposalRefusal::ClockUnhealthy)
		);
	}
	let Ok(WendelonRequest::Decision(DecisionCommand::RecordNoop { stamp_ms, .. })) =
		admit(mode, noop_sender, noop, &trusted(), healthy)
	else {
		panic!("a RecordNoop");
	};
	assert_eq!(stamp_ms, fixture::T0);
	let claim = fixture::claim(
		fixture::key(fixture::T0, 3),
		fixture::children(&state, 1),
		0,
	);
	let DecisionCommand::Abort { abort } = fixture::abort_command(&claim, 1) else {
		panic!("an Abort");
	};
	let recovery = [
		(
			coordinator,
			ForwardedDecision::Finalize {
				key: claim.key,
				request_digest: claim.request_digest,
				acks: Vec::new(),
			},
		),
		(
			coordinator,
			ForwardedDecision::Resolve {
				key: claim.key,
				request_digest: claim.request_digest,
			},
		),
		(
			abort.body.coordinator.node,
			ForwardedDecision::Abort { abort },
		),
		(
			1,
			ForwardedDecision::BindSigner {
				incarnation: fixture::inc(1),
			},
		),
		(
			1,
			ForwardedDecision::RegisterIncarnation {
				incarnation: fixture::inc(1),
			},
		),
	];
	for (sender, request) in recovery {
		assert!(
			matches!(
				admission(mode, sender, request.clone(), &trusted()),
				Ok(Admitted::Ready(_))
			),
			"{request:?}"
		);
		assert!(
			admit(mode, sender, request.clone(), &trusted(), unhealthy).is_ok(),
			"{request:?} needs no time"
		);
	}
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

/// ADR 0700: the decision clock gate needs fresh samples from floor(n/2)
/// other current voters (3, 5 and 7 voters need 1, 2 and 3), counted and
/// medianed over voters only; self, non-voters, duplicates and stale samples
/// never count; a skewed peer median refuses; a joint configuration and a
/// leader outside the voters refuse; a single voter has no peer median.
#[test]
fn the_decision_clock_gate_needs_a_fresh_voter_majority() {
	use std::collections::BTreeSet;
	use std::time::Duration;
	let now = fixture::T0;
	let fresh = Duration::from_millis(100);
	let stale = CLOCK_SAMPLE_FRESHNESS + Duration::from_millis(1);
	let voters = |n: u32| (1..=n).collect::<BTreeSet<u32>>();
	let samples = |peers: &[u32], age: Duration| {
		peers
			.iter()
			.map(|peer| (*peer, now - age.as_millis() as u64, age))
			.collect::<Vec<_>>()
	};
	for (n, needed) in [(3u32, 1usize), (5, 2), (7, 3)] {
		let peers: Vec<u32> = (2..=n).collect();
		assert_eq!(
			clock_gate(
				now,
				1,
				Some(&voters(n)),
				&samples(&peers[..needed - 1], fresh)
			),
			Err(ProposalRefusal::ClockUnhealthy),
			"{n} voters, {} fresh of {needed} needed",
			needed - 1
		);
		assert_eq!(
			clock_gate(now, 1, Some(&voters(n)), &samples(&peers[..needed], fresh)),
			Ok(now),
			"{n} voters, {needed} fresh"
		);
		assert_eq!(
			clock_gate(now, 1, Some(&voters(n)), &samples(&peers[..needed], stale)),
			Err(ProposalRefusal::ClockUnhealthy),
			"{n} voters, only stale samples"
		);
	}
	// Self, a non-voter and a duplicate do not count toward the majority.
	let mut padded = samples(&[1, 9], fresh);
	padded.extend(samples(&[2], fresh));
	padded.extend(samples(&[2], fresh));
	assert_eq!(
		clock_gate(now, 1, Some(&voters(5)), &padded),
		Err(ProposalRefusal::ClockUnhealthy),
		"one distinct eligible voter of two needed"
	);
	// A peer median skewed beyond the bound refuses.
	let skewed = vec![
		(2, now - MAX_OFFSET_MS - 200, fresh),
		(3, now - MAX_OFFSET_MS - 200, fresh),
	];
	assert_eq!(
		clock_gate(now, 1, Some(&voters(3)), &skewed),
		Err(ProposalRefusal::ClockUnhealthy)
	);
	// A sample's age is monotonic: a later wall-clock jump forward does not
	// make a stale sample count, and the projection follows its age.
	assert_eq!(
		clock_gate(now + 60_000, 1, Some(&voters(3)), &samples(&[2], stale)),
		Err(ProposalRefusal::ClockUnhealthy)
	);
	// Joint configurations and a leader that is not a voter are refused.
	assert_eq!(
		clock_gate(now, 1, None, &samples(&[2, 3], fresh)),
		Err(ProposalRefusal::ClockUnhealthy)
	);
	assert_eq!(
		clock_gate(now, 4, Some(&voters(3)), &samples(&[2, 3], fresh)),
		Err(ProposalRefusal::ClockUnhealthy)
	);
	// A single voter has no peer median.
	assert_eq!(clock_gate(now, 1, Some(&voters(1)), &[]), Ok(now));
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

/// ADR 0700 (r1 R3): a held probe never outlives its owner. Stopping cancels
/// and awaits it; dropping the owner, as an aborted maintenance loop does,
/// cancels it; either way the in-flight guard is released.
#[tokio::test]
async fn a_held_clock_probe_is_cancelled_by_stop_or_by_its_owner_dropping() {
	struct Ended(Option<tokio::sync::oneshot::Sender<()>>);
	impl Drop for Ended {
		fn drop(&mut self) {
			let _ = self.0.take().unwrap().send(());
		}
	}
	let held = || {
		let (tx, rx) = tokio::sync::oneshot::channel();
		let ended = Ended(Some(tx));
		(
			async move {
				let _ended = ended;
				std::future::pending::<()>().await;
			},
			rx,
		)
	};

	let mut probe = ClockProbe::default();
	let (future, mut ended) = held();
	probe.start(future);
	tokio::task::yield_now().await;
	assert!(probe.busy(), "a held probe occupies its owner");
	assert!(ended.try_recv().is_err());
	probe.stop().await;
	assert!(ended.try_recv().is_ok(), "stop cancelled and awaited it");
	assert!(!probe.busy(), "the guard is released");

	let (future, ended) = held();
	probe.start(future);
	tokio::task::yield_now().await;
	drop(probe);
	tokio::time::timeout(std::time::Duration::from_secs(5), ended)
		.await
		.expect("dropping the owner cancels the probe")
		.unwrap();
}
