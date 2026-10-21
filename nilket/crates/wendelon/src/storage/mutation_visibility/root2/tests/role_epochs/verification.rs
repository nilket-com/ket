use super::*;
#[test]
fn legacy_admin_statement_epoch0_after_rotation_denied8() {
	let mut f = F::new();
	let old = f.legacy_statement();
	assert!(check_admin_statement2(&f.installed().view(), &old).is_ok());
	f.rotate(0, 40);
	deny(f.f.apply(Command2::AdmissionPermission(old), None), 8);
	assert_eq!(f.root().state.permissions.len(), 0);
}
#[test]
fn legacy_governance_sidecar_epoch0_after_rotation_denied8() {
	let mut f = F::new();
	let c = wire::enroll_authority();
	let old = f.f.envelope(&f.f.state, &c, 12);
	assert!(
		super::super::super::governance_command::check_governance_command2(
			&f.installed().view(),
			super::super::super::governance_command::GovernanceAction2::EnrollAuthority,
			&old,
			c.digest(f.f.dep.generation, f.f.dep.cluster).unwrap()
		)
		.is_ok()
	);
	f.rotate(1, 40);
	deny(f.apply(c, old), 8);
	assert_eq!(f.root().state.authorities.len(), 0);
}
/// The epoch-0 statement path (VISADS02, which replaced the removed VISADC01 primitive) verifies only under the
/// original administrator key, never the successor's, once the administrator has rotated.
#[test]
fn legacy_admin_statement_never_uses_successor_key() {
	let mut f = F::new();
	let old = f.legacy_statement();
	assert!(check_admin_statement2(&f.installed().view(), &old).is_ok());
	f.rotate(0, 40);
	assert_eq!(
		check_admin_statement2(&f.installed().view(), &old).err(),
		Some(StatementError2::Signature)
	);
	let unsigned: [u8; 337] = [&old[..273], &[0; 64][..]].concat().try_into().unwrap();
	let sig = SigningKey::from_bytes(&[40; 32])
		.sign(&admin_statement_preimage2(&unsigned).unwrap())
		.to_bytes();
	let changed: [u8; 337] = [&old[..273], &sig[..]].concat().try_into().unwrap();
	assert_eq!(
		check_admin_statement2(&f.installed().view(), &changed).err(),
		Some(StatementError2::Signature)
	);
}
#[test]
fn explicit_admin_permission_current_epoch_and_consumer_join() {
	let mut f = F::new();
	f.enroll();
	f.rotate(0, 40);
	let c = Command2::EpochAdmissionPermission(f.statement());
	assert!(matches!(
		f.f.apply(c, None),
		ApplyResult2::Permission { outcome: 1, .. }
	));
	assert_eq!(f.root().state.permissions.len(), 1);
	f.roundtrip();
}
#[test]
fn explicit_governance_all_nine_current_epoch_paths() {
	let mut f = F::new();
	f.rotate(1, 40);
	let issuer = Command2::EnrollLeaseIssuer {
		node: 7,
		key: key(wire::ISSUER_SEED),
	};
	let p = f.proof(&issuer);
	f.apply(issuer, p);
	for c in wire::nine(&f.f) {
		let p = f.proof(&c);
		assert_eq!(
			super::super::super::governance_verification::verify_governance2(
				&f.f.state,
				&f.f.pin,
				&c,
				Some(&p),
				c.digest(f.f.dep.generation, f.f.dep.cluster).unwrap()
			),
			Ok(true)
		);
	}
}
#[test]
fn canonical_missing_wrong_duplicate_extra_ordered_proofs_denied8() {
	for mode in 0..5 {
		let mut f = F::new();
		let c = F::command(2, 40);
		let mut p = match mode {
			1 => f.proof_as(&c, [3, 0]),
			2 => f.proof_as(&c, [1, 1]),
			3 => f.proof_as(&c, [2, 1]),
			_ => f.proof(&c),
		};
		if mode == 0 {
			p[85] = 0;
			p[86..150].fill(0);
		}
		if mode == 4 {
			p[159] = 0;
			p[160..224].fill(0);
		}
		assert!(role_command::parse(&p).is_ok());
		deny(f.apply(c, p), 8);
	}
	let mut f = F::new();
	let c = F::command(0, 40);
	let p = f.proof_as(&c, [3, 1]);
	deny(f.apply(c, p), 8);
}
#[test]
fn old_future_and_forged_role_epochs_denied8() {
	for epoch in [1u64, 65, u64::MAX] {
		let mut f = F::new();
		let c = F::command(0, 40);
		let mut p = f.proof(&c);
		p[77..85].copy_from_slice(&epoch.to_le_bytes());
		deny(f.apply(c, p), 8);
	}
	let mut f = F::new();
	let c = F::command(0, 40);
	let mut p = f.proof(&c);
	p[86] ^= 1;
	deny(f.apply(c, p), 8);
	let old = f.statement();
	f.rotate(0, 41);
	deny(f.f.apply(Command2::EpochAdmissionPermission(old), None), 8);
}
#[test]
fn malformed_carrier_sidecar_precedes_profile_and_auth() {
	let f = F::new();
	let c = F::command(0, 40);
	let p = f.proof(&c);
	use crate::storage::mutation_visibility::root2::committed::{
		CommittedSource2, committed_entry2,
	};
	for length in [0, 223, 225, 295] {
		let side = vec![0; length];
		let e = wire::carrier(&c, f.f.dep, 3, Some(&side));
		let adapted = committed_entry2(&CommittedSource2::synthetic(), &e).unwrap();
		assert!(adapted.committed().entry_fields().2 == EntryClass2::MalformedRoot2);
		assert!(adapted.into_parts().2.is_none());
	}
	let mut bad = p.clone();
	bad[10] = 1;
	for side in [&p, &bad] {
		let e = wire::carrier(&c, f.f.dep, 3, Some(side));
		let adapted = committed_entry2(&CommittedSource2::synthetic(), &e).unwrap();
		assert!(matches!(
			adapted.committed().entry_fields().2,
			EntryClass2::OtherRoot2(_)
		));
	}
	let e = wire::carrier(&c, f.f.dep, 3, Some(&bad));
	let result = wire::step(&f.f.state, &f.f.pin, f.f.dep, &e, &c, Some(&bad));
	assert!(matches!(result, Err(VerifyError2::Role(_))));
	let e = wire::carrier(&c, f.f.dep, 3, None);
	assert!(matches!(
		wire::step(&f.f.state, &f.f.pin, f.f.dep, &e, &c, None),
		Err(VerifyError2::MissingSidecar)
	));
}
#[test]
fn installed_view_stale_cut_cannot_reach_admission_effect() {
	let mut f = F::new();
	let stale = f.installed();
	f.rotate(0, 40);
	let c = Command2::EpochAdmissionPermission(f.statement());
	let e = super::super::super::trust::TrustedEntry2::fixture(
		f.f.dep.generation,
		f.f.dep.cluster,
		wire::log(4),
		&c,
	)
	.unwrap();
	let v = super::super::super::trust::VerifiedCommand2::fixture(
		f.f.dep.generation,
		f.f.dep.cluster,
		&c,
		&[],
		0,
	)
	.unwrap();
	assert_eq!(
		super::super::super::Root2::reduce_admission(&f.f.state, &stale, &e, &v, &c).err(),
		Some(RootError2::TrustMismatch)
	);
}
#[test]
fn historical_mixed_prefix_replays_identical_effects_and_denials() {
	let mut f = F::new();
	let genesis = F::new().f.state;
	assert_eq!(wire::bytes(&genesis), wire::bytes(&f.f.state));
	let start = f.f.entries.len();
	f.enroll();
	for (role, seed) in [(0, 40), (1, 41), (2, 42), (0, 43)] {
		f.rotate(role, seed);
		let c = F::command(role, 11);
		let p = f.proof(&c);
		deny(f.apply(c, p), 21);
	}
	let mut state = genesis;
	for e in &f.f.entries[start..] {
		let adapted = super::super::super::committed::committed_entry2(
			&super::super::super::committed::CommittedSource2::synthetic(),
			e,
		)
		.unwrap();
		let (_, _, carrier) = adapted.into_parts();
		let (_, _, c, p) = carrier.unwrap().into_parts();
		state = wire::step(&state, &f.f.pin, f.f.dep, e, &c, p).unwrap().0;
	}
	assert_eq!(wire::bytes(&state), wire::bytes(&f.f.state));
}
#[test]
fn root_pin_original_keys_and_role_ids_never_rotate() {
	let mut f = F::new();
	let pin = super::super::super::provisioning::replica_anchor::tuple_manifest_pin2(&f.f.pin);
	let ctx = admin_context2(&f.installed());
	let id = admin_id2(&f.installed());
	for role in 0..3 {
		f.rotate(role, 40 + role as u8);
	}
	assert_eq!(admin_context2(&f.installed()), ctx);
	assert_eq!(admin_id2(&f.installed()), id);
	assert_eq!(
		super::super::super::provisioning::replica_anchor::tuple_manifest_pin2(&f.f.pin),
		pin
	);
	assert_eq!(
		original_roles3(&f.installed()),
		Some([key(11), key(12), key(13)])
	);
}
