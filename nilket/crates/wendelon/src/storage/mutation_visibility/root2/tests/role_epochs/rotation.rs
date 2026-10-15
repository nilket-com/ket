use super::*;
#[test]
fn recovery_rotates_admin_exact_pre_entry_boundary() {
	let mut f = F::new();
	let before = f.installed();
	f.rotate(0, 40);
	assert_eq!(active_role3(&before, 0), Some((0, key(11))));
	assert_eq!(active_role3(&f.installed(), 0), Some((1, key(40))));
	f.roundtrip();
}
#[test]
fn recovery_rotates_governance_exact_pre_entry_boundary() {
	let mut f = F::new();
	f.rotate(1, 41);
	let c = wire::enroll_authority();
	let p = f.proof(&c);
	assert!(matches!(f.apply(c, p), ApplyResult2::Registered(_)));
	assert_eq!(active_role3(&f.installed(), 1), Some((1, key(41))));
}
#[test]
fn joint_admin_governance_rotates_recovery_boundary() {
	let mut f = F::new();
	f.rotate(2, 42);
	let c = F::command(0, 43);
	let p = f.proof(&c);
	assert!(matches!(f.apply(c, p), ApplyResult2::Registered(_)));
	assert_eq!(active_role3(&f.installed(), 2), Some((1, key(42))));
}
#[test]
fn forbidden_cross_rotation_and_self_rotation_are_denied8() {
	for (target, roles) in [
		(0, [1, 0]),
		(1, [1, 0]),
		(0, [2, 0]),
		(1, [2, 0]),
		(2, [3, 0]),
	] {
		let mut f = F::new();
		let c = F::command(target, 44);
		let p = f.proof_as(&c, roles);
		deny(f.apply(c, p), 8);
		assert!(f.root().state.role_epochs.is_none());
	}
}
#[test]
fn payload_weak_reused_limit_are_exact20_21_22() {
	let mut f = F::new();
	let c = Command2::RotateAdmin([0; 32]);
	let p = f.proof(&c);
	deny(f.apply(c, p), 20);
	let c = F::command(0, 12);
	let p = f.proof(&c);
	deny(f.apply(c, p), 21);
	for i in 0..64 {
		f.rotate(0, 32 + i);
	}
	let c = F::command(0, 96);
	let p = f.proof(&c);
	deny(f.apply(c, p), 22);
}
#[test]
fn payload_checks_follow_authorization() {
	let mut f = F::new();
	for k in [[0; 32], key(12)] {
		let c = Command2::RotateAdmin(k);
		let p = f.proof_as(&c, [1, 0]);
		deny(f.apply(c, p), 8);
	}
	assert!(f.root().state.role_epochs.is_none());
}
#[test]
fn all_roles_stop_at64_successors_and_no_overflow() {
	let mut f = F::new();
	for role in 0..3 {
		for i in 0..64 {
			f.rotate(role, (32 + role * 64 + i) as u8);
		}
		let c = F::command(role, 224 + role as u8);
		let p = f.proof(&c);
		deny(f.apply(c, p), 22);
		f.roundtrip();
	}
	assert_eq!(f.root().usage().unwrap().history, 10104);
}
#[test]
fn cross_role_and_historical_key_reuse_are_denied21() {
	let mut f = F::new();
	for role in [0, 2] {
		let first = F::command(role, 50 + role as u8);
		let proof = f.proof(&first);
		assert!(matches!(
			f.apply(first.clone(), proof.clone()),
			ApplyResult2::Registered(_)
		));
		f.seeds[role] = 50 + role as u8;
		deny(f.apply(first.clone(), proof.clone()), 21);
		f.rotate(role, 60 + role as u8);
		deny(f.apply(first, proof), 21);
	}
	for role in 0..3 {
		for seed in [11, 12, 13, 50, 52] {
			let c = F::command(role, seed);
			let p = f.proof(&c);
			deny(f.apply(c, p), 21);
		}
	}
}
#[test]
fn history_charge_default_and_exact_minimum_budget() {
	for (budget, success) in [(0, false), (171, false), (172, true)] {
		let mut f = F::new();
		f.f.state = lower_limits_for_test(
			&f.f.state,
			Limits2 {
				history: budget,
				..Limits2::default()
			},
		);
		let c = F::command(0, 40);
		let p = f.proof(&c);
		let r = f.apply(c, p);
		if success {
			assert!(matches!(r, ApplyResult2::Registered(_)));
			assert_eq!(f.root().usage().unwrap().history, 172);
		} else {
			deny(r, 11);
			assert!(f.root().state.role_epochs.is_none());
		}
	}
}
#[test]
fn capacity_denial11_rolls_back_seed_key_and_epoch() {
	let mut f = F::new();
	f.rotate(0, 40);
	f.f.state = lower_limits_for_test(
		&f.f.state,
		Limits2 {
			history: 223,
			..Limits2::default()
		},
	);
	let old = f.root().state.role_epochs.clone();
	let c = F::command(1, 41);
	let p = f.proof(&c);
	deny(f.apply(c, p), 11);
	assert!(f.root().state.role_epochs == old);
	assert_eq!(f.root().usage().unwrap().history, 172);
}
#[test]
fn epoch_activation_joins_non_grants_and_coord_owners() {
	let mut f = F::new();
	f.rotate(0, 40);
	let r = f.root();
	assert!(r.state.derived.non_grants.contains_key(&3));
	let mut s = r.state.clone();
	s.role_epochs.as_mut().unwrap().successors[0][0].activated = s.bootstrap;
	assert_eq!(
		super::super::super::validation::validate_state(&s).err(),
		Some(RootError2::RootInvariant)
	);
}
#[test]
fn rotation_replay_same_entry_and_changed_fingerprint() {
	let mut f = F::new();
	let c = F::command(0, 40);
	let p = f.proof(&c);
	f.apply(c.clone(), p.clone());
	let e = f.f.entries.last().unwrap();
	let (next, r) = wire::step(&f.f.state, &f.f.pin, f.f.dep, e, &c, Some(&p)).unwrap();
	assert!(r == ApplyResult2::AlreadyProcessed);
	assert_eq!(wire::bytes(&next), wire::bytes(&f.f.state));
	let ce = super::super::super::committed::committed_entry2(
		&super::super::super::committed::CommittedSource2::synthetic(),
		e,
	)
	.unwrap()
	.into_committed();
	let mut te = super::super::super::trust::TrustedEntry2::bind(
		&ce,
		&c,
		f.f.dep.generation,
		f.f.dep.cluster,
	)
	.unwrap();
	te.change_fingerprint();
	assert!(advance_verified2(&f.f.state, &f.f.pin, &ce, &te, &c, Some(&p)).is_err());
}
