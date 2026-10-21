//! `owner_core.rs:196`: a Reserve whose manifest does not encode is refused as `Codec` by the
//! shared `request_key2`, before any state. Only a profile-1 manifest can be built invalid, so this
//! row keeps its profile-1 input until c2 (IMPL-NOTES b2 ruling); it now runs through the only
//! owner core, `v2::decide2`. The other typed-request rows are in `owner_core_three.rs`.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::v2::decide2;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::*;

fn op(n: u8) -> OpId2 {
	op_id2([n; 16]).unwrap()
}
#[test]
fn invalid_typed_requests_are_refused_before_any_state() {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("profile_three_vectors.json")).unwrap();
	let AnyAdminManifest2::One(base) =
		AnyAdminManifest2::decode(&unhex(v["manifests"][0].as_str().unwrap())).unwrap()
	else {
		unreachable!("manifests[0] is the profile-1 manifest")
	};
	let log = p3_log_bytes();
	let codec = Decision2::Refuse(OwnerError2::Codec);
	let mut m = base.clone();
	m.initial_epoch = 0;
	let reserve = |m| OwnerRequest2::Reserve {
		manifest: AnyAdminManifest2::One(m),
	};
	assert_eq!(decide2(&log, &op(0x41), &reserve(m.clone())), codec);
	let mut k = base;
	k.admin_key = [0; 32];
	assert_eq!(decide2(&log, &op(0x41), &reserve(k)), codec);
	// Invalid input is refused even on a log that is not clean.
	assert_eq!(decide2(&log[..10], &op(0x41), &reserve(m)), codec);
}
