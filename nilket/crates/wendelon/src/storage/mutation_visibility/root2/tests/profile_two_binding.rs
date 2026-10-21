//! The profile-1/2 manifests "a" and "b" (inlined) for the bridge users that remain until c2, and
//! the {137, 189, 241} governed marker, which is state-only. The profile-1/2 install and chain tests
//! went at c1: a profile-1/2 tuple can no longer be built (the snapshot encoder and the tuple refuse
//! them), so those paths are unreachable end to end.
use super::profile_three_fixture::{M3_PRIMARY, chain_of};
use super::*;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::validation::admission_permission;

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
/// Independent M2 manifests: "a" profile 1 (admin seed 0x21); "b" profile 2 (admin seed 0x42,
/// governance seed 0x92).
// Inlined from the deleted provisioning/tests/profile_two_inventory_vectors.json (c1); the
// profile-1/2 users of these two manifests go in c2.
const MANIFEST_A: &str = concat!(
	"56495341444d320001000100212121212121212121212121212121212e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e",
	"210100000000000012121212121212121212121212121212884b8857f4eaa1613c61504db34d4beaf346517a",
	"0e31de3cddd4d9b4201d9d0b0300000005000000000000000000000000000000000000000000000000000000",
	"0000000000",
);
const MANIFEST_B: &str = concat!(
	"56495341444d320001000200424242424242424242424242424242424d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d",
	"4201000000000000717171717171717171717171717171712152f8d19b791d24453242e15f2eab6cb7cffa7b",
	"6a5ed30097960e069881db120300000005000000000000000000000000000000000000000000000000000000",
	"000000000091919191919191919191919191919191c6d0c796bb4c3c923615f8dc85e6fc35149eb6b1317de5",
	"23d3a5085584f5c109ff010000",
);
pub(super) fn manifest_bytes(name: &str) -> Vec<u8> {
	unhex(match name {
		"a" => MANIFEST_A,
		"b" => MANIFEST_B,
		other => panic!("no inlined manifest {other}"),
	})
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
	// The marker is state-only: any rooted state carries it (a profile-1/2 tuple no longer builds).
	let base = chain_of(&M3_PRIMARY, 241).rooted;
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
