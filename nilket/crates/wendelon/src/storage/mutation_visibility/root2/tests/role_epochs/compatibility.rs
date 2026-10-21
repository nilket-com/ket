use super::*;
#[test]
fn all_old_profiles_and_no_extension_inverse_proof() {
	let f = F::new();
	for root in [f.f.state] {
		let r = root_and_origin2(&root).unwrap().0;
		assert_eq!(
			r.encode().unwrap(),
			super::super::super::state::encode_state_reference(&r.state).unwrap()
		);
		assert!(r.state.role_epochs.is_none());
	}
}
#[test]
fn owner_inventory_snapshot_and_tuple_identity_stable() {
	let mut f = F::new();
	let original_manifest = f.f.manifest.clone();
	let original_record =
		super::super::super::provisioning::replica_anchor::tuple_record2(&f.f.pin);
	let original_pin =
		super::super::super::provisioning::replica_anchor::tuple_manifest_pin2(&f.f.pin);
	f.rotate(0, 40);
	f.rotate(1, 41);
	f.rotate(2, 42);
	assert_eq!(f.f.manifest, original_manifest);
	assert_eq!(
		super::super::super::provisioning::replica_anchor::tuple_record2(&f.f.pin),
		original_record
	);
	assert_eq!(
		super::super::super::provisioning::replica_anchor::tuple_manifest_pin2(&f.f.pin),
		original_pin
	);
	assert_eq!(
		original_roles3(&f.installed()),
		Some([key(11), key(12), key(13)])
	);
}
#[test]
fn carrier_wal5_v4_fixtures_and_caps_unchanged() {
	use crate::consensus::*;
	assert_eq!(ROOT2_GOVERNANCE_MAX1, 296);
	assert_eq!(ROOT2_ENTRY_MAX1, 262522);
	let f = F::new();
	let c = F::command(0, 40);
	let p = f.proof(&c);
	let e = wire::carrier(&c, f.f.dep, 3, Some(&p));
	let b = postcard::to_stdvec(&e).unwrap();
	let decoded: wire::E = postcard::from_bytes(&b).unwrap();
	assert_eq!(postcard::to_stdvec(&decoded).unwrap(), b);
	let manifest =
		include_str!("../../../../../../tests/fixtures/consensus_body/wal_v4_entries.txt");
	let hex = manifest
		.lines()
		.find_map(|l| l.strip_prefix("finalize="))
		.unwrap();
	let expected: Vec<u8> = (0..hex.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
		.collect();
	for version in [0, 4] {
		let legacy =
			crate::consensus::raft_storage::carrier_legacy_fixture_entry(version, "finalize")
				.unwrap();
		assert_eq!(postcard::to_stdvec(&legacy).unwrap(), expected);
	} /* Current carrier roundtrip is not an additional WAL decoder proof; unchanged full WAL corpus tests retain that obligation. */
}
#[test]
fn new_layout_old_reader_refuses_without_authority() {
	let mut f = F::new();
	f.rotate(0, 40);
	let b = f.root().encode().unwrap();
	assert_eq!(&b[..8], b"VISAUT05");
	assert!(super::super::s1c_oracle::install(&b).is_err());
}
#[test]
fn ordinary_release_has_no_role_epoch_test_symbols() {
	let modules = include_str!("../../mod.rs");
	assert!(modules.contains("#[cfg(test)]\nmod tests;"));
	let tests = include_str!("../mod.rs");
	assert!(tests.contains("mod role_epochs;"));
	for module in ["codec", "rotation", "verification", "compatibility"] {
		assert!(include_str!("mod.rs").contains(&format!("mod {module};")));
	} /* Actual release-symbol absence is independently checked in the selected release gate; this test pins cfg ancestry only. */
}
#[test]
fn enrolled_authority_issuer_and_holder_keys_not_rotated() {
	let mut f = F::new();
	f.enroll();
	let c = Command2::EnrollLeaseIssuer {
		node: 7,
		key: key(wire::ISSUER_SEED),
	};
	let p = f.proof(&c);
	f.apply(c, p);
	let authorities = f.root().state.authorities.clone();
	let issuers = f.root().state.issuers.clone();
	for role in 0..3 {
		f.rotate(role, 40 + role as u8);
	}
	assert!(f.root().state.authorities == authorities);
	assert!(f.root().state.issuers == issuers);
	assert_eq!(f.root().state.parents.len(), 0);
}
#[test]
fn root_decode_is_unverified_and_prefix_replay_is_separate() {
	let mut f = F::new();
	f.rotate(0, 40);
	let b = f.root().encode().unwrap();
	let parsed = super::super::super::UnverifiedRoot2::decode(&b).unwrap();
	assert_eq!(format!("{parsed:?}"), "UnverifiedRoot2 { redacted }");
	assert_eq!(parsed.encode().unwrap(), b);
	assert_eq!(
		super::super::super::origin::encode_authority2(&f.f.state).unwrap(),
		wire::bytes(&f.f.state)
	);
}
#[test]
fn successful_rotation_registered_result_and_denial_codec_roundtrip() {
	let mut f = F::new();
	let c = F::command(0, 40);
	let p = f.proof(&c);
	let result = f.apply(c, p);
	assert!(result == ApplyResult2::Registered(wire::log(3)));
	for r in [
		result,
		ApplyResult2::Denied {
			reason: 20,
			reference: None,
		},
		ApplyResult2::Denied {
			reason: 21,
			reference: None,
		},
		ApplyResult2::Denied {
			reason: 22,
			reference: None,
		},
	] {
		let b = r
			.encode(f.f.dep.generation, f.f.dep.cluster, wire::log(3))
			.unwrap();
		let (g, c, l, round) = ApplyResult2::decode(&b).unwrap();
		assert_eq!(
			(g, c, l),
			(f.f.dep.generation, f.f.dep.cluster, wire::log(3))
		);
		assert!(round == r);
	}
	for reason in 20..=22 {
		assert!(
			ApplyResult2::Denied {
				reason,
				reference: Some(wire::log(2))
			}
			.encode(f.f.dep.generation, f.f.dep.cluster, wire::log(3))
			.is_err()
		);
	}
}
