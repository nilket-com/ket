//! M3 isolated length refusals, superseding the M1 dormancy refusals with coherent fixtures: on
//! each profile's separately coherent wrong-length chain C's Bootstrap check and install refuse
//! on the length alone; observe accepts exactly the two canonical lengths; the issuer gate refuses
//! a state whose only difference is `control_len`. Fixture builders are in `profile_two_binding`.
use super::profile_three_fixture::{
	control_len, enrolled, expected_context, grant, pre_root, reduce_at, rooted,
};
use super::profile_two_binding::{
	ADMISSION_SEED_A, ADMISSION_SEED_B, PROFILES, chain, manifest_bytes, non_length_conjuncts,
};
use super::*;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::verification::*;

/// C1/C4: on each profile's wrong-length chain every non-length conjunct holds against its own
/// tuple, entry and command, and C refuses; the separately coherent right-length chain is accepted
/// and reaches exactly the fixture-built root.
#[test]
fn c_bootstrap_length_is_isolated_on_two_coherent_chains() {
	for (name, right, wrong) in PROFILES {
		let good = chain(name, right);
		let (got, _) = advance_verified2(
			&pre_root(),
			&good.tuple,
			&good.committed,
			&good.entry,
			&good.cmd,
			None,
		)
		.unwrap();
		assert_eq!(
			encode_authority2(&got).unwrap(),
			encode_authority2(&good.rooted).unwrap(),
			"{name}"
		);
		let bad = chain(name, wrong);
		assert_eq!(non_length_conjuncts(&bad), [true; 5], "{name}");
		assert_eq!(tuple_manifest_len2(&bad.tuple), right, "{name}");
		assert_eq!(
			advance_verified2(
				&pre_root(),
				&bad.tuple,
				&bad.committed,
				&bad.entry,
				&bad.cmd,
				None
			)
			.err(),
			Some(VerifyError2::Consumer(RootError2::TrustMismatch)),
			"{name}"
		);
		// The two chains are separately coherent: the length changes the bound command digest.
		assert_ne!(
			tuple_record2(&good.tuple).identity.bootstrap_command,
			tuple_record2(&bad.tuple).identity.bootstrap_command
		);
	}
}

/// C2/C4: install on each chain's own root and tuple; the wrong chain differs only in length.
#[test]
fn install_length_is_isolated_on_two_coherent_chains() {
	for (name, right, wrong) in PROFILES {
		let good = chain(name, right);
		let admin = install_admin2(&good.rooted, &good.tuple).unwrap();
		assert_eq!(admin_manifest_len2(&admin), right);
		let bad = chain(name, wrong);
		let (root, _) = root_and_origin2(&bad.rooted).unwrap();
		assert_eq!(root.state.control_digest, tuple_manifest_pin2(&bad.tuple));
		assert_eq!(
			install_admin2(&bad.rooted, &bad.tuple).err(),
			Some(InstallError2::Commitment(CommitmentField2::ControlLength)),
			"{name}"
		);
	}
}

/// Observe stays state-only and descriptive: either canonical length, nothing else.
#[test]
fn observe_accepts_exactly_the_two_canonical_lengths() {
	// Frozen historical name: C-S4a adds Three and moves unknown classes before rooting.
	let bytes = manifest_bytes("a");
	for length in [137, 189, 241] {
		assert!(observe_bootstrap2(&rooted(&bytes, length).3).is_ok());
	}
	assert_eq!(
		observe_bootstrap2(&rooted(&bytes, 0).3).err(),
		Some(ObserveError2::ControlLength)
	);
	let original = root("root_bootstrap_only");
	let (g, c) = (original.state.generation, original.state.cluster);
	for length in [136, 138, 188, 190] {
		let mut cmd = Command2::Bootstrap {
			epoch: 1,
			control: [7; 32],
			length: 137,
		};
		let Command2::Bootstrap { length: field, .. } = &mut cmd else {
			unreachable!()
		};
		*field = length;
		let e = TrustedEntry2::fixture(g, c, l(1), &cmd).unwrap();
		let v = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
		assert_eq!(
			Root2::reduce(None, &e, &v, &cmd).err(),
			Some(RootError2::ControlClass),
			"{length}"
		);
	}
}

/// C3: the gate compares the root's commitment length with the installed recorded length. Only
/// `control_len` differs in the mutated state; the whole expected context still equals the
/// administrator's, so the refusal is the length check alone.
#[test]
fn gate_length_is_isolated_by_a_control_len_only_change() {
	for (name, right, other) in PROFILES {
		let c = chain(name, right);
		let admin = install_admin2(&c.rooted, &c.tuple).unwrap();
		let seed = if name == "a" {
			ADMISSION_SEED_A
		} else {
			ADMISSION_SEED_B
		};
		let state = enrolled(&c.rooted, &c.any);
		let cmd = grant(&admin, seed);
		assert!(
			matches!(
				reduce_at(&state, &admin, &cmd, 4),
				Ok(ApplyResult2::Permission { .. })
			),
			"{name}"
		);
		let mutated = with_control_len_for_test(&state, other);
		assert_eq!(control_len(&mutated), other);
		assert!(admin_context2(&admin) == expected_context(&mutated));
		assert_eq!(
			reduce_at(&mutated, &admin, &cmd, 4).err(),
			Some(RootError2::TrustMismatch),
			"{name}"
		);
	}
}
