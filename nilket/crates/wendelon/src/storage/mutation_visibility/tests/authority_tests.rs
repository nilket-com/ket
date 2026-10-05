use super::*;
use std::sync::{Arc, Barrier};

#[test]
fn first_terminal_is_immutable_in_both_orders() {
	for first in [
		TerminalDecision::Accepted,
		TerminalDecision::Refused(RefusalCause::EpochCut),
	] {
		let a = authority();
		let original = record(a.apply(&event(2, 1, first)).unwrap());
		for (i, terminal) in [
			TerminalDecision::Accepted,
			TerminalDecision::Refused(RefusalCause::InvalidAdmission),
		]
		.into_iter()
		.enumerate()
		{
			let replay = record(a.apply(&event(3 + i as u64, 1, terminal)).unwrap());
			assert_eq!(replay, original);
		}
		assert_eq!(
			a.usage().unwrap(),
			AuthorityUsage {
				decisions: 1,
				stored_bytes: 274
			}
		);
		assert_eq!(a.lookup(&identity(1)).unwrap(), Some(original));
	}
}
#[test]
fn conflicting_context_leaves_first_decision_and_advances_only_coverage() {
	let a = authority();
	let old = record(a.apply(&event(2, 1, TerminalDecision::Accepted)).unwrap());
	for (i, field) in (0..5).enumerate() {
		let mut m = manifest(1);
		let mut commitment = [8; 32];
		match field {
			0 => m.admission.epoch = 2,
			1 => m.key_version = 2,
			2 => m.children[0].versions[0].content_commitment = [9; 32],
			3 => commitment = [9; 32],
			_ => m.children[0].range_end = 101,
		};
		let e = AuthorityEvent::Decision {
			log: log(3 + i as u64),
			manifest: m,
			commitment,
			terminal: TerminalDecision::Accepted,
		};
		assert_eq!(a.apply(&e).unwrap(), ApplyOutcome::Conflict);
		assert_eq!(a.lookup(&identity(1)).unwrap(), Some(old.clone()));
		assert_eq!(a.applied().unwrap(), log(3 + i as u64));
	}
	let before = a.encode_extension().unwrap();
	let mut bad = manifest(2);
	bad.identity.generation = [99; 16];
	assert_eq!(
		a.apply(&AuthorityEvent::Decision {
			log: log(20),
			manifest: bad,
			commitment: [8; 32],
			terminal: TerminalDecision::Accepted
		}),
		Err(VisibilityError::WrongGeneration)
	);
	assert_eq!(a.encode_extension().unwrap(), before);
}
#[test]
fn same_caller_sequence_is_independent_per_tenant_in_both_orders() {
	for tenants in [[7, 8], [8, 7]] {
		let a = authority();
		let mut expected = Vec::new();
		for (i, tenant) in tenants.into_iter().enumerate() {
			let mut m = manifest(1);
			m.identity.tenant = tenant;
			let id = m.identity;
			let r = record(
				a.apply(&AuthorityEvent::Decision {
					log: log(2 + i as u64),
					manifest: m,
					commitment: [8; 32],
					terminal: TerminalDecision::Accepted,
				})
				.unwrap(),
			);
			expected.push((id, r));
		}
		assert_eq!(a.usage().unwrap().decisions, 2);
		for (id, r) in expected {
			assert_eq!(a.lookup(&id).unwrap(), Some(r));
		}
	}
}
#[test]
fn capacity_has_no_fake_terminal_and_replays_still_work() {
	for a in [
		authority().with_limits(1, MAX_STORED_DECISION_BYTES),
		authority().with_limits(MAX_DECISIONS, 274),
	] {
		let old = record(
			a.apply(&event(
				2,
				1,
				TerminalDecision::Refused(RefusalCause::ExplicitPolicy),
			))
			.unwrap(),
		);
		assert_eq!(
			a.apply(&event(3, 2, TerminalDecision::Accepted)).unwrap(),
			ApplyOutcome::CapacityUnavailable
		);
		assert_eq!(a.lookup(&identity(2)).unwrap(), None);
		assert_eq!(a.applied().unwrap(), log(3));
		assert_eq!(a.usage().unwrap().decisions, 1);
		assert_eq!(
			record(a.apply(&event(4, 1, TerminalDecision::Accepted)).unwrap()),
			old
		);
	}
	let a = authority().with_limits(1, 273);
	assert_eq!(
		a.apply(&event(2, 1, TerminalDecision::Accepted)).unwrap(),
		ApplyOutcome::CapacityUnavailable
	);
	assert_eq!(a.usage().unwrap().stored_bytes, 0);
}
#[test]
fn full_default_count_limit_is_enforced_and_readable() {
	let a = authority();
	for sequence in 0..MAX_DECISIONS as u64 {
		assert!(matches!(
			a.apply(&event(sequence + 2, sequence, TerminalDecision::Accepted))
				.unwrap(),
			ApplyOutcome::Recorded(_)
		));
	}
	assert_eq!(
		a.usage().unwrap(),
		AuthorityUsage {
			decisions: MAX_DECISIONS,
			stored_bytes: 8978432
		}
	);
	assert_eq!(
		a.apply(&event(
			MAX_DECISIONS as u64 + 2,
			MAX_DECISIONS as u64,
			TerminalDecision::Accepted
		))
		.unwrap(),
		ApplyOutcome::CapacityUnavailable
	);
	assert!(matches!(
		a.apply(&event(
			MAX_DECISIONS as u64 + 3,
			0,
			TerminalDecision::Accepted
		))
		.unwrap(),
		ApplyOutcome::Replayed(_)
	));
	let encoded = a.encode_extension().unwrap();
	let restored = DecisionAuthority::decode_extension(&encoded, [1; 16]).unwrap();
	assert_eq!(restored.encode_extension().unwrap(), encoded);
}
#[test]
fn latest_event_fingerprint_and_log_order_are_checked() {
	let a = authority();
	let e = event(2, 1, TerminalDecision::Accepted);
	a.apply(&e).unwrap();
	assert_eq!(a.apply(&e).unwrap(), ApplyOutcome::AlreadyProcessed);
	assert_eq!(
		a.apply(&event(2, 2, TerminalDecision::Accepted)),
		Err(VisibilityError::CoordinateConflict)
	);
	assert_eq!(
		a.apply(&event(1, 2, TerminalDecision::Accepted)),
		Err(VisibilityError::UnorderedReplay)
	);
	assert_eq!(
		a.apply(&AuthorityEvent::Coverage { log: log(3) }).unwrap(),
		ApplyOutcome::Covered
	);
	assert_eq!(a.usage().unwrap().decisions, 1);
}
#[test]
fn concurrent_first_decision_orders_are_barrier_pinned() {
	for first in [
		TerminalDecision::Accepted,
		TerminalDecision::Refused(RefusalCause::EpochCut),
	] {
		let a = Arc::new(authority());
		let gate = Arc::new(Barrier::new(2));
		let release = Arc::new(Barrier::new(2));
		let aa = a.clone();
		let gg = gate.clone();
		let rr = release.clone();
		let thread = std::thread::spawn(move || {
			gg.wait();
			rr.wait();
			record(
				aa.apply(&event(
					3,
					1,
					TerminalDecision::Refused(RefusalCause::ExplicitPolicy),
				))
				.unwrap(),
			)
		});
		gate.wait();
		let original = record(a.apply(&event(2, 1, first)).unwrap());
		release.wait();
		assert_eq!(thread.join().unwrap(), original);
	}
	// Truly simultaneous proposals at one coordinate: either wins, the other conflicts.
	let a = Arc::new(authority());
	let barrier = Arc::new(Barrier::new(3));
	let mut tasks = Vec::new();
	for terminal in [
		TerminalDecision::Accepted,
		TerminalDecision::Refused(RefusalCause::EpochCut),
	] {
		let aa = a.clone();
		let bb = barrier.clone();
		tasks.push(std::thread::spawn(move || {
			bb.wait();
			aa.apply(&event(2, 1, terminal))
		}));
	}
	barrier.wait();
	let results: Vec<_> = tasks.into_iter().map(|t| t.join().unwrap()).collect();
	assert_eq!(
		results
			.iter()
			.filter(|r| matches!(r, Ok(ApplyOutcome::Recorded(_))))
			.count(),
		1
	);
	assert_eq!(
		results
			.iter()
			.filter(|r| **r == Err(VisibilityError::CoordinateConflict))
			.count(),
		1
	);
	assert_eq!(a.usage().unwrap().decisions, 1);
}
#[test]
fn serial_seeded_model_preserves_first_records() {
	use std::collections::BTreeMap;
	let a = authority().with_limits(32, 100000);
	let mut model = BTreeMap::new();
	let mut seed = 100u64;
	for index in 2..2002 {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		let sequence = seed % 64;
		let terminal = if seed & 1 == 0 {
			TerminalDecision::Accepted
		} else {
			TerminalDecision::Refused(RefusalCause::ExplicitPolicy)
		};
		let outcome = a.apply(&event(index, sequence, terminal)).unwrap();
		if let Some(old) = model.get(&sequence) {
			assert_eq!(record(outcome), *old);
		} else if model.len() < 32 {
			model.insert(sequence, record(outcome));
		} else {
			assert_eq!(outcome, ApplyOutcome::CapacityUnavailable);
		}
		assert_eq!(a.usage().unwrap().decisions, model.len());
	}
	for (sequence, r) in model {
		assert_eq!(a.lookup(&identity(sequence)).unwrap(), Some(r));
	}
}
#[test]
fn poisoned_authority_is_unavailable_not_empty() {
	let a = Arc::new(authority());
	let aa = a.clone();
	let _ = std::thread::spawn(move || {
		let _guard = aa.state.lock().unwrap();
		panic!("synthetic lock poison");
	})
	.join();
	assert_eq!(a.lookup(&identity(1)), Err(VisibilityError::Poisoned));
	assert_eq!(a.encode_extension(), Err(VisibilityError::Poisoned));
}
#[test]
fn measure_resident_components_and_encoded_sizes() {
	let a = authority();
	a.apply(&event(2, 1, TerminalDecision::Accepted)).unwrap();
	let s = a.state.lock().unwrap();
	let r = s.records.values().next().unwrap();
	let child = &r.manifest.children[0];
	println!(
		"SIZE DecisionAuthority={} AuthorityState={} DecisionRecord={} map_key={} child={} version={} child_vec_capacity={} row_vec_capacity={} stored_record={} extension={}",
		std::mem::size_of::<DecisionAuthority>(),
		std::mem::size_of::<authority::AuthorityState>(),
		std::mem::size_of::<DecisionRecord>(),
		73,
		std::mem::size_of::<ManifestChild>(),
		std::mem::size_of::<LogicalVersion>(),
		r.manifest.children.capacity(),
		child.versions.capacity(),
		r.stored_bytes().unwrap(),
		92 + 274 + 46
	);
	// Actual allocator requests, rather than claiming canonical byte size is a heap bound.
	drop(s);
	assert_eq!(a.encode_extension().unwrap().len(), 412);
}
