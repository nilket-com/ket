//! M3 coherent fixture chains (one per profile and Bootstrap length) and the positive binding
//! surface: Admission-only signing on a profile-2 install, the derived installed profile/length,
//! the {137, 189} governed marker and shared-Admission-key pin binding. The isolated length
//! refusals on these chains are in `profile_two_refusal`. The committed, trusted and
//! verified builders are test-only FIXTURES; the checked paths they feed are the published ones.
//! Manifests come from the independent M2 vectors; entry fingerprints and command digests are
//! Rust-derived through those fixtures.
use super::profile_three_fixture::{
	Chain, chain_of, enrolled, exact_pin, genesis, grant, reduce_at,
};
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, ManifestProfile2,
};
use crate::storage::mutation_visibility::root2::admin_command::*;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::admin_statement::*;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::validation::admission_permission;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
/// Independent M2 manifests: "a" profile 1 (admin seed 0x21); "b" profile 2 (admin seed 0x42,
/// governance seed 0x92).
pub(super) fn manifest_bytes(name: &str) -> Vec<u8> {
	let v: serde_json::Value = serde_json::from_str(include_str!(
		"../provisioning/tests/profile_two_inventory_vectors.json"
	))
	.unwrap();
	unhex(v["manifests"][name]["bytes"].as_str().unwrap())
}
pub(super) const ADMISSION_SEED_A: u8 = 0x21;
pub(super) const ADMISSION_SEED_B: u8 = 0x42;
pub(super) const GOVERNANCE_SEED_B: u8 = 0x92;
pub(super) fn chain(name: &str, length: u64) -> Chain {
	chain_of(&manifest_bytes(name), length)
}
/// The published non-length conjuncts of `bootstrap_authorized`, restated test-side.
pub(super) fn non_length_conjuncts(c: &Chain) -> [bool; 5] {
	let Command2::Bootstrap { epoch, control, .. } = &c.cmd else {
		unreachable!()
	};
	let (g, cl, l, fp, d) = c.entry.fields();
	let r = tuple_record2(&c.tuple).identity;
	[
		(r.generation, r.cluster, r.bootstrap) == (g, cl, l),
		(r.bootstrap_fingerprint, r.bootstrap_command) == (fp, d),
		r.initial_epoch == *epoch,
		r.genesis_fingerprint == genesis(),
		*control == tuple_manifest_pin2(&c.tuple),
	]
}
pub(super) const PROFILES: [(&str, u64, u64); 2] = [("a", 137, 189), ("b", 189, 137)];

/// r2 §5: a profile-2 administrator verifies statements and envelopes under its Admission key
/// only; the governance key's valid signatures over the same preimages are refused.
#[test]
fn a_profile_two_install_verifies_under_the_admission_key_only() {
	let c = chain("b", 189);
	let admin = install_admin2(&c.rooted, &c.tuple).unwrap();
	assert_eq!(admin_profile2(&admin), ManifestProfile2::Two);
	let AnyAdminManifest2::Two(m) = &c.any else {
		unreachable!()
	};
	let role = admin_governance2(&admin).unwrap();
	assert!(role == m.governance());
	let governance = SigningKey::from_bytes(&[GOVERNANCE_SEED_B; 32]);
	assert_eq!(governance.verifying_key().to_bytes(), role.key());
	assert_eq!(
		SigningKey::from_bytes(&[ADMISSION_SEED_B; 32])
			.verifying_key()
			.to_bytes(),
		admin_key2(&admin)
	);
	// Statements.
	let Command2::AdmissionPermission(by_admission) = grant(&admin, ADMISSION_SEED_B) else {
		unreachable!()
	};
	let Command2::AdmissionPermission(by_governance) = grant(&admin, GOVERNANCE_SEED_B) else {
		unreachable!()
	};
	assert_eq!(
		by_admission[..273],
		by_governance[..273],
		"same signed bytes"
	);
	let preimage = admin_statement_preimage2(&by_governance).unwrap();
	let sig = ed25519_dalek::Signature::from_bytes(&by_governance[273..].try_into().unwrap());
	VerifyingKey::from_bytes(&role.key())
		.unwrap()
		.verify_strict(&preimage, &sig)
		.expect("the governance signature is valid over the same preimage");
	assert!(check_admin_statement2(&admin.view(), &by_admission).is_ok());
	assert_eq!(
		check_admin_statement2(&admin.view(), &by_governance).err(),
		Some(StatementError2::Signature)
	);
	// Envelopes.
	let ctx = admin_context2(&admin);
	let digest = [0x5A; 32];
	let unsigned = encode_admin_command2(&ctx, digest, &[0; 64]).unwrap();
	let preimage = admin_command_preimage2(&unsigned).unwrap();
	let envelope = |seed: u8| {
		let s = SigningKey::from_bytes(&[seed; 32])
			.sign(&preimage)
			.to_bytes();
		encode_admin_command2(&ctx, digest, &s).unwrap()
	};
	assert!(check_admin_command2(&admin, &envelope(ADMISSION_SEED_B), digest).is_ok());
	assert_eq!(
		check_admin_command2(&admin, &envelope(GOVERNANCE_SEED_B), digest).err(),
		Some(EnvelopeError2::Signature)
	);
	// The issuer gate: Admission-signed Grant applies; the governance-signed one is a consumed
	// statement denial.
	let state = enrolled(&c.rooted, &c.any);
	assert!(matches!(
		reduce_at(&state, &admin, &grant(&admin, ADMISSION_SEED_B), 4),
		Ok(ApplyResult2::Permission { .. })
	));
	assert!(matches!(
		reduce_at(&state, &admin, &grant(&admin, GOVERNANCE_SEED_B), 4),
		Ok(ApplyResult2::Denied { reason: 8, .. })
	));
	// A profile-1 administrator has no governance role.
	let p1 = chain("a", 137);
	let admin = install_admin2(&p1.rooted, &p1.tuple).unwrap();
	assert_eq!(
		(admin_profile2(&admin), admin_governance2(&admin)),
		(ManifestProfile2::One, None)
	);
}

/// The installed profile, length and governance are one derivation, for both profiles.
#[test]
fn installed_profile_length_and_governance_agree() {
	for (name, right, _) in PROFILES {
		let c = chain(name, right);
		let admin = install_admin2(&c.rooted, &c.tuple).unwrap();
		let two = admin_governance2(&admin).is_some();
		assert_eq!(admin_profile2(&admin) == ManifestProfile2::Two, two);
		assert_eq!(admin_manifest_len2(&admin) == 189, two);
		assert_eq!(admin_manifest_len2(&admin), tuple_manifest_len2(&c.tuple));
		assert_eq!(admin_profile2(&admin), c.any.profile());
		let a = c.any.admission();
		assert_eq!(
			(admin_id2(&admin), admin_key2(&admin)),
			(a.admin_id, a.admin_key)
		);
		assert!(admin_namespace2(&admin) == a.namespace);
		assert_eq!(
			admin_context2(&admin).manifest_pin,
			exact_pin(&manifest_bytes(name))
		);
	}
}

/// r1 §5: governed iff the commitment length is 137 or 189 (an immutable state-only marker).
#[test]
fn admission_governed_marker_is_either_canonical_length() {
	let v: serde_json::Value = serde_json::from_str(include_str!("reserve_vectors.json")).unwrap();
	// The first generated Reserve claim; its validity is not under test, only the marker.
	let claim = v["commands"]
		.as_object()
		.unwrap()
		.values()
		.find_map(
			|h| match Command2::decode(&unhex(h.as_str().unwrap())).unwrap().2 {
				Command2::Reserve(claim) => Some(claim),
				_ => None,
			},
		)
		.unwrap();
	let base = chain("b", 189).rooted;
	for (length, governed) in [
		(137, true),
		(189, true),
		(241, true),
		(0, false),
		(136, false),
		(138, false),
		(188, false),
		(190, false),
	] {
		let s = with_control_len_for_test(&base, length);
		let state = &root_and_origin2(&s).unwrap().0.state;
		if !matches!(length, 0 | 137 | 189 | 241) {
			assert_eq!(
				admission_permission(state, &claim, 5),
				Err(RootError2::ControlClass)
			);
			continue;
		}
		let got = admission_permission(state, &claim, 5).unwrap();
		assert_eq!(got.is_some(), governed, "{length}");
		if governed {
			assert_eq!(got, Some((18, None)), "{length}");
		}
	}
}

/// M2 item 6 carried into M3: two coherent profile-2 manifests sharing the Admission fields
/// (and key) but not the governance id or key have their own pins; a tuple never installs against
/// the other root (pin binding, not Admission-key substitution). A different mask is no manifest.
#[test]
fn shared_admission_key_never_substitutes_across_roots() {
	for other in ["b_governance_id", "b_governance_key"] {
		let b = chain("b", 189);
		let x = chain(other, 189);
		assert_eq!(
			(
				x.any.admission().admin_key,
				x.any.admission().generation,
				x.any.admission().cluster
			),
			(
				b.any.admission().admin_key,
				b.any.admission().generation,
				b.any.admission().cluster
			)
		);
		assert_ne!(tuple_manifest_pin2(&x.tuple), tuple_manifest_pin2(&b.tuple));
		assert!(install_admin2(&x.rooted, &x.tuple).is_ok());
		assert_eq!(
			install_admin2(&b.rooted, &x.tuple).err(),
			Some(InstallError2::Commitment(CommitmentField2::ControlDigest)),
			"{other}"
		);
		assert_eq!(
			install_admin2(&x.rooted, &b.tuple).err(),
			Some(InstallError2::Commitment(CommitmentField2::ControlDigest)),
			"{other}"
		);
	}
	let mut mask = manifest_bytes("b");
	mask[185..189].copy_from_slice(&0xFFu32.to_le_bytes());
	assert_eq!(
		AnyAdminManifest2::decode(&mask).err(),
		Some(crate::storage::mutation_visibility::root2::admin::errors::AdminManifestError2::UnsupportedAction)
	);
}
