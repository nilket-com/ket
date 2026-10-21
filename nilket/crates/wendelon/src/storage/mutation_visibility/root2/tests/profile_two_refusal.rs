//! M3 isolated length refusals: observe accepts exactly the two canonical lengths, and the issuer
//! gate refuses a state whose only difference is `control_len` (on the profile-3 chain since c1,
//! where a profile-1/2 tuple can no longer be built; the C and install length rows are the profile-3
//! ports in `verification.rs` and `owner_tuple_delivery.rs`).
use super::profile_three_fixture::{
	M3_PRIMARY, PRIMARY_SEEDS, chain_of, control_len, enrolled, expected_context, grant, reduce_at,
	rooted,
};
use super::profile_two_binding::manifest_bytes;
use super::*;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::origin::*;

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
	// On the profile-3 chain (a profile-1/2 tuple no longer builds), with the off-by-one lengths.
	for other in [240, 242] {
		let name = other;
		let c = chain_of(&M3_PRIMARY, 241);
		let state = enrolled(&c.rooted, &c.any);
		// A profile-3 administrator is installed at the tip it acts on.
		let admin = install_admin2(&state, &c.tuple).unwrap();
		let seed = PRIMARY_SEEDS[0];
		let cmd = grant(&admin, seed);
		let first = reduce_at(&state, &admin, &cmd, 4);
		assert!(
			matches!(first, Ok(ApplyResult2::Permission { .. })),
			"{name}: {first:?}"
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
