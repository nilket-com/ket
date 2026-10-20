//! T-INV: the three profile-3 length gates that `validate_state` already rules out are kept as
//! invariants. Each is reached by mutating `control_len` after validation, with every other input
//! correct; the same call at 241 is the positive twin.
use super::governance_verification::support::Fixture;
use super::profile_three_fixture::{M3_PRIMARY, PRIMARY_SEEDS, key};
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::origin::root_and_origin2;
use crate::storage::mutation_visibility::root2::role_epochs::Seed3;
use crate::storage::mutation_visibility::root2::types::{Log2, RootError2};
use crate::storage::mutation_visibility::root2::{Root2, retirement, role_epochs, role_freeze};

/// The validated `M3_PRIMARY` root and the exact seed its pre-entry cut yields.
fn root() -> (Root2, Seed3) {
	let f = Fixture::profile_three(&M3_PRIMARY);
	let root = root_and_origin2(&f.state).unwrap().0.clone();
	let s = &root.state;
	let seed = (
		PRIMARY_SEEDS.map(key),
		s.covered,
		s.fingerprint,
		s.command_digest,
	);
	(root, seed)
}
fn at(root: &Root2) -> Log2 {
	Log2 {
		index: root.state.covered.index + 1,
		..root.state.covered
	}
}
/// `f` with the root's `control_len` set to `len`: the result of the gated call.
fn gated<T>(len: u64, f: impl Fn(&mut Root2, Seed3) -> T) -> T {
	let (mut root, seed) = root();
	root.state.control_len = len;
	f(&mut root, seed)
}
fn refused<T>(f: impl Fn(&mut Root2, Seed3) -> Result<T, RootError2>) {
	assert!(gated(241, &f).is_ok(), "positive twin at 241");
	for len in [0, 240, 242] {
		assert_eq!(
			gated(len, &f).err(),
			Some(RootError2::TrustMismatch),
			"{len}"
		);
	}
}

#[test]
fn rotation_refuses_a_non_profile_three_length() {
	refused(|root: &mut Root2, seed| {
		let at = at(root);
		role_epochs::rotate(root, 0, key(40), at, Some(seed))
	});
	let r = gated(241, |root: &mut Root2, seed| {
		let at = at(root);
		role_epochs::rotate(root, 0, key(40), at, Some(seed))
	});
	assert!(matches!(r, Ok(ApplyResult2::Registered(_))));
}
#[test]
fn freeze_transition_refuses_a_non_profile_three_length() {
	let zero = Log2 {
		term: 0,
		node: 0,
		index: 0,
	};
	refused(|root: &mut Root2, seed| {
		let at = at(root);
		role_freeze::transition(root, &Command2::Freeze(zero), at, Some(seed))
	});
}
#[test]
fn retirement_refuses_a_non_profile_three_length() {
	refused(|root: &mut Root2, seed| {
		let at = at(root);
		retirement::retire(root, at, [0x71; 32], [0x72; 32], Some(seed))
	});
}
