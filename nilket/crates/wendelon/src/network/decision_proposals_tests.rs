//! ADR 0699 S3a: what the leader admits from a forwarded request.

use super::*;
use crate::consensus::decisions::tests as fixture;
use crate::consensus::raft_storage::apply_at_leader_for_test;

/// A stand-in for the key a store signs with, where the step only carries it.
const KEY: [u8; 32] = [0x5A; 32];

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
		key: trusted()[&1],
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
				key: trusted()[&1],
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
		key: trusted()[&2],
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
			ForwardedDecision::BindSigner {
				incarnation,
				key: trusted()[&2],
			},
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
		maintenance_step(None, false, mine, KEY),
		Some(ForwardedDecision::RegisterIncarnation { incarnation: mine })
	);
	assert_eq!(
		maintenance_step(Some(mine), false, mine, KEY),
		Some(ForwardedDecision::BindSigner {
			incarnation: mine,
			key: KEY,
		})
	);
	assert_eq!(maintenance_step(Some(mine), true, mine, KEY), None);
	// A different established store at this node id is never retired from here.
	assert_eq!(maintenance_step(Some(other), false, mine, KEY), None);
	assert_eq!(maintenance_step(Some(other), true, mine, KEY), None);
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
		key: trusted()[&1],
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

/// ADR 0702: a time-bearing admission waits, bounded by one absolute deadline,
/// for the unchanged gate to pass. Each case runs on paused time.
mod clock_admission_wait {
	use super::*;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

	fn deadline() -> tokio::time::Instant {
		tokio::time::Instant::now() + CLOCK_ADMISSION_WAIT
	}

	#[tokio::test(start_paused = true)]
	async fn a_sample_arriving_during_the_wait_admits_it() {
		let arrived = Arc::new(tokio::sync::Notify::new());
		let healthy = Arc::new(AtomicBool::new(false));
		let (signal, flag) = (arrived.clone(), healthy.clone());
		tokio::spawn(async move {
			tokio::time::sleep(std::time::Duration::from_millis(300)).await;
			flag.store(true, Ordering::SeqCst);
			signal.notify_waiters();
		});
		let started = tokio::time::Instant::now();
		let outcome = wait_for_decision_clock(
			&arrived,
			deadline(),
			|| {
				let healthy = healthy.clone();
				async move {
					if healthy.load(Ordering::SeqCst) {
						Ok(fixture::T0)
					} else {
						Err(ProposalRefusal::ClockUnhealthy)
					}
				}
			},
			|| async { true },
		)
		.await;
		assert_eq!(outcome, ClockWait::Stamp(fixture::T0));
		assert_eq!(started.elapsed(), std::time::Duration::from_millis(300));
	}

	#[tokio::test(start_paused = true)]
	async fn no_sample_refuses_at_the_deadline_without_a_stamp() {
		let arrived = tokio::sync::Notify::new();
		let started = tokio::time::Instant::now();
		let outcome = wait_for_decision_clock(
			&arrived,
			deadline(),
			|| async { Err(ProposalRefusal::ClockUnhealthy) },
			|| async { true },
		)
		.await;
		assert_eq!(outcome, ClockWait::Refused(ProposalRefusal::ClockUnhealthy));
		assert_eq!(started.elapsed(), CLOCK_ADMISSION_WAIT);
	}

	#[tokio::test(start_paused = true)]
	async fn a_real_skew_still_refuses_at_the_same_deadline() {
		// Samples keep arriving but disagree: each wake re-evaluates, and the
		// deadline is never renewed.
		let arrived = Arc::new(tokio::sync::Notify::new());
		let signal = arrived.clone();
		tokio::spawn(async move {
			loop {
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
				signal.notify_waiters();
			}
		});
		let evaluations = Arc::new(AtomicUsize::new(0));
		let started = tokio::time::Instant::now();
		let outcome = wait_for_decision_clock(
			&arrived,
			deadline(),
			|| {
				evaluations.fetch_add(1, Ordering::SeqCst);
				async { Err(ProposalRefusal::ClockUnhealthy) }
			},
			|| async { true },
		)
		.await;
		assert_eq!(outcome, ClockWait::Refused(ProposalRefusal::ClockUnhealthy));
		assert_eq!(started.elapsed(), CLOCK_ADMISSION_WAIT);
		assert!(
			evaluations.load(Ordering::SeqCst) > 10,
			"re-evaluated on each sample"
		);
	}

	#[tokio::test(start_paused = true)]
	async fn another_refusal_returns_at_once() {
		let arrived = tokio::sync::Notify::new();
		let started = tokio::time::Instant::now();
		let outcome = wait_for_decision_clock(
			&arrived,
			deadline(),
			|| async { Err(ProposalRefusal::NotTheSender) },
			|| async { true },
		)
		.await;
		assert_eq!(outcome, ClockWait::Refused(ProposalRefusal::NotTheSender));
		assert_eq!(started.elapsed(), std::time::Duration::ZERO);
	}

	#[tokio::test(start_paused = true)]
	async fn a_gate_held_past_the_deadline_never_stamps() {
		// The gate's own evaluation blocks (a contended state read) and then
		// succeeds late, or exactly at the deadline: no stamp either way.
		for late in [3_000u64, 2_000] {
			let arrived = tokio::sync::Notify::new();
			let started = tokio::time::Instant::now();
			let outcome = wait_for_decision_clock(
				&arrived,
				deadline(),
				|| async move {
					tokio::time::sleep(std::time::Duration::from_millis(late)).await;
					Ok(fixture::T0)
				},
				|| async { true },
			)
			.await;
			assert_eq!(
				outcome,
				ClockWait::Refused(ProposalRefusal::ClockUnhealthy),
				"late {late}"
			);
			assert_eq!(started.elapsed(), CLOCK_ADMISSION_WAIT, "late {late}");
		}
	}

	#[tokio::test(start_paused = true)]
	async fn a_leadership_check_held_past_the_deadline_never_stamps() {
		let arrived = tokio::sync::Notify::new();
		let started = tokio::time::Instant::now();
		let outcome = wait_for_decision_clock(
			&arrived,
			deadline(),
			|| async { Ok(fixture::T0) },
			|| async {
				tokio::time::sleep(std::time::Duration::from_millis(3_000)).await;
				true
			},
		)
		.await;
		assert_eq!(outcome, ClockWait::Refused(ProposalRefusal::ClockUnhealthy));
		assert_eq!(started.elapsed(), CLOCK_ADMISSION_WAIT);
	}

	#[tokio::test(start_paused = true)]
	async fn leadership_lost_while_waiting_proposes_nothing() {
		let arrived = Arc::new(tokio::sync::Notify::new());
		let healthy = Arc::new(AtomicBool::new(false));
		let (signal, flag) = (arrived.clone(), healthy.clone());
		tokio::spawn(async move {
			tokio::time::sleep(std::time::Duration::from_millis(200)).await;
			flag.store(true, Ordering::SeqCst);
			signal.notify_waiters();
		});
		let outcome = wait_for_decision_clock(
			&arrived,
			deadline(),
			|| {
				let healthy = healthy.clone();
				async move {
					if healthy.load(Ordering::SeqCst) {
						Ok(fixture::T0)
					} else {
						Err(ProposalRefusal::ClockUnhealthy)
					}
				}
			},
			|| async { false },
		)
		.await;
		assert_eq!(outcome, ClockWait::NotLeader);
	}
}

/// ADR 0707 (acceptance 4), the admission check: a stop already set is seen
/// before the proposal is polled, so nothing is submitted.
#[tokio::test]
async fn a_ready_stop_returns_stopped_without_polling_the_proposal() {
	let (_stop, stopped) = tokio::sync::watch::channel(true);
	let never_polled = std::future::poll_fn(|_| -> std::task::Poll<ProposalResponse> {
		panic!("a proposal was polled after the stop")
	});
	assert!(matches!(
		until_stopped(stopped, never_polled).await,
		MaintenanceProposalResult::Stopped
	));
}

/// ADR 0707: a stop set while the proposal waits ends the wait, however long
/// the answer would take.
#[tokio::test]
async fn a_stop_during_the_wait_ends_it() {
	let (stop, stopped) = tokio::sync::watch::channel(false);
	let setter = tokio::spawn(async move {
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		stop.send_replace(true);
		stop
	});
	let result = tokio::time::timeout(
		std::time::Duration::from_secs(10),
		until_stopped(stopped, std::future::pending::<ProposalResponse>()),
	)
	.await
	.expect("the stop ends the wait");
	assert!(matches!(result, MaintenanceProposalResult::Stopped));
	drop(setter.await.unwrap());
}

/// ADR 0707: without a stop the answer is returned as it came, and a closed
/// stop channel never stops a wait.
#[tokio::test]
async fn without_a_stop_the_answer_is_returned() {
	let answer = || async {
		tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		ProposalResponse::NotLeader {
			leader_hint: Some(7),
		}
	};
	let (_stop, stopped) = tokio::sync::watch::channel(false);
	assert!(matches!(
		until_stopped(stopped, answer()).await,
		MaintenanceProposalResult::Response(ProposalResponse::NotLeader {
			leader_hint: Some(7)
		})
	));
	let (stop, stopped) = tokio::sync::watch::channel(false);
	drop(stop);
	assert!(matches!(
		until_stopped(stopped, answer()).await,
		MaintenanceProposalResult::Response(ProposalResponse::NotLeader {
			leader_hint: Some(7)
		})
	));
}

// ADR 0721

#[test]
fn a_binding_whose_key_disagrees_with_the_leaders_map_binds_nothing() {
	let mode = DecisionMode::ParentDecisions;
	let disagreeing = ForwardedDecision::BindSigner {
		incarnation: fixture::inc(2),
		key: trusted()[&3],
	};
	assert_eq!(
		admit(mode, 2, disagreeing, &trusted(), || panic!(
			"a binding reads no clock"
		)),
		Err(ProposalRefusal::SignerKeyDisagrees)
	);
	// The sender's key never stands in for the leader's own entry: a sender the
	// leader has no key for is refused whatever key it sends.
	let unknown = ForwardedDecision::BindSigner {
		incarnation: fixture::inc(2),
		key: trusted()[&2],
	};
	assert_eq!(
		admit(mode, 9, unknown, &trusted(), healthy),
		Err(ProposalRefusal::UnknownSignerKey)
	);
	// The refusal is appended to the wire enum after the existing ones.
	assert_eq!(
		postcard::to_stdvec(&ProposalRefusal::SignerKeyDisagrees).unwrap(),
		vec![5]
	);
	assert_eq!(
		postcard::to_stdvec(&ProposalRefusal::Malformed).unwrap(),
		vec![4]
	);
}

#[test]
fn an_answer_is_judged_by_type_and_frame_version_before_its_body() {
	let header = |msg_type: MsgType, version: u8| FrameHeader {
		magic: HRFT_MAGIC,
		version,
		msg_type: msg_type.0,
		flags: 0,
		stream_id: 0,
		meta_len: 0,
		arrow_len: 0,
		trailing_len: 0,
	};
	let envelope = DecisionMode::ParentDecisions.raft_frame_version();
	let (propose, lookup) = (
		MsgType::DECISION_PROPOSE_RESP,
		MsgType::DECISION_LOOKUP_RESP,
	);
	check_response_frame(&header(propose, envelope), propose, envelope).unwrap();
	// An ADR 0720 leader's answer, a Today-versioned answer and an error frame.
	assert!(check_response_frame(&header(propose, 9), propose, envelope).is_err());
	assert!(check_response_frame(&header(propose, 1), propose, envelope).is_err());
	assert!(check_response_frame(&header(lookup, envelope), propose, envelope).is_err());
	// Lookups keep the one HRFT version in both directions.
	check_response_frame(&header(lookup, 1), lookup, 1).unwrap();
	assert!(check_response_frame(&header(lookup, envelope), lookup, 1).is_err());
}
