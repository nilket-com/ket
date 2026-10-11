//! Profile-2 manifest codec (M1, additive and dormant). Every byte comes from the independent
//! reference `profile_two_reference.py`; the bad keys are the existing independent
//! `negative_keys`, applied to EACH role. The profile-1 API is shown unchanged alongside.
use super::*;
use AdminManifestError2::*;

fn p2() -> serde_json::Value {
	serde_json::from_str(include_str!("profile_two_vectors.json")).unwrap()
}
fn p2_valid(name: &str) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
	let v = p2()["valid"]
		.as_array()
		.unwrap()
		.iter()
		.find(|x| x["name"] == name)
		.unwrap()
		.clone();
	(
		hex(v["bytes"].as_str().unwrap()),
		hex(v["digest"].as_str().unwrap()),
		hex(v["profile_one"].as_str().unwrap()),
	)
}
fn error_of(name: &str) -> AdminManifestError2 {
	match name {
		"OverReadBound" => OverReadBound,
		"Length" => Length,
		"Magic" => Magic,
		"UnsupportedFormat" => UnsupportedFormat,
		"UnsupportedProfile" => UnsupportedProfile,
		"ProfileLength" => ProfileLength,
		"ZeroField" => ZeroField,
		"UnsupportedAction" => UnsupportedAction,
		"Namespace" => Namespace,
		"RoleReuse" => RoleReuse,
		"NonCanonicalKey" => NonCanonicalKey,
		other => panic!("unknown error {other}"),
	}
}
fn decode_two(b: &[u8]) -> Result<AdminManifestTwo2, AdminManifestError2> {
	let before = b.to_vec();
	let r = AdminManifestTwo2::decode(b);
	assert_eq!(b, &before[..], "input never modified");
	r
}
fn edited(b: &[u8], at: usize, v: &[u8]) -> Vec<u8> {
	let mut out = b.to_vec();
	out[at..at + v.len()].copy_from_slice(v);
	out
}

#[test]
fn valid_profile_two_round_trips_digests_and_exposes_its_parts() {
	let gov_id: [u8; 16] = hex(p2()["governance_id"].as_str().unwrap())
		.try_into()
		.unwrap();
	let gov_key: [u8; 32] = hex(p2()["governance_key"].as_str().unwrap())
		.try_into()
		.unwrap();
	for name in ["raw", "table"] {
		let (b, digest, one) = p2_valid(name);
		let m = decode_two(&b).unwrap();
		assert_eq!(m.encode().unwrap().to_vec(), b, "{name}");
		let fixed: [u8; ADMIN_MANIFEST_P2_BYTES2] = b.clone().try_into().unwrap();
		assert_eq!(admin_manifest_two_digest2(&fixed).to_vec(), digest);
		assert_eq!(manifest_profile2(&b), Ok(ManifestProfile2::Two));
		// The borrowed Admission subview equals the profile-1 manifest it was built from.
		assert!(*m.base() == AdminManifest2::decode(&one).unwrap());
		let g = m.governance();
		assert_eq!((g.id(), g.key(), g.actions()), (gov_id, gov_key, 0x1FF));
		// The constructors build the same value.
		let built = AdminManifestTwo2::new(
			AdminManifest2::decode(&one).unwrap(),
			GovernanceRole2::new(gov_id, gov_key).unwrap(),
		)
		.unwrap();
		assert!(built == m);
		assert_eq!(built.encode().unwrap().to_vec(), b);
	}
}

#[test]
fn every_error_vector_matches_the_reference_order() {
	let errors = p2()["errors"].as_array().unwrap().clone();
	assert_eq!(errors.len(), 34);
	for e in errors {
		let name = e["name"].as_str().unwrap();
		let b = hex(e["bytes"].as_str().unwrap());
		let want = error_of(e["error"].as_str().unwrap());
		assert_eq!(decode_two(&b).err(), Some(want), "{name}");
		// The structural peek answers exactly steps 1-6.
		let peek = manifest_profile2(&b);
		match want {
			OverReadBound | Length | Magic | UnsupportedFormat | ProfileLength => {
				assert_eq!(peek, Err(want), "{name}")
			}
			UnsupportedProfile if name == "profile_1_valid_137" => {
				assert_eq!(peek, Ok(ManifestProfile2::One))
			}
			UnsupportedProfile => assert_eq!(peek, Err(UnsupportedProfile), "{name}"),
			_ => assert_eq!(peek, Ok(ManifestProfile2::Two), "{name}"),
		}
	}
}

/// The existing independent negative keys, in the Admission slot and in the governance slot.
#[test]
fn every_negative_key_is_refused_for_each_role() {
	let (raw, _, one) = p2_valid("raw");
	let keys = vectors()["negative_keys"].as_array().unwrap().clone();
	assert_eq!(keys.len(), 13);
	let base = AdminManifest2::decode(&one).unwrap();
	let gov_id: [u8; 16] = raw[137..153].try_into().unwrap();
	let gov_key: [u8; 32] = raw[153..185].try_into().unwrap();
	for k in keys {
		let key: [u8; 32] = hex(k["key"].as_str().unwrap()).try_into().unwrap();
		assert_eq!(
			decode_two(&edited(&raw, 68, &key)).err(),
			Some(NonCanonicalKey)
		);
		assert_eq!(
			decode_two(&edited(&raw, 153, &key)).err(),
			Some(NonCanonicalKey)
		);
		assert_eq!(
			GovernanceRole2::new(gov_id, key).err(),
			Some(NonCanonicalKey)
		);
		let mut bad_base = base.clone();
		bad_base.admin_key = key;
		assert_eq!(
			AdminManifestTwo2::new(bad_base, GovernanceRole2::new(gov_id, gov_key).unwrap()).err(),
			Some(NonCanonicalKey)
		);
	}
}

#[test]
fn constructors_validate_in_their_documented_order() {
	let (raw, _, one) = p2_valid("raw");
	let base = AdminManifest2::decode(&one).unwrap();
	let gov_key: [u8; 32] = raw[153..185].try_into().unwrap();
	let gov = GovernanceRole2::new([0x6c; 16], gov_key).unwrap();
	// GovernanceRole2::new: a zero id before the key.
	assert_eq!(
		GovernanceRole2::new([0; 16], [0; 32]).err(),
		Some(ZeroField)
	);
	// AdminManifestTwo2::new: base ZeroField, then Namespace, then RoleReuse, then keys.
	let mut zero = base.clone();
	zero.generation = [0; 16];
	assert_eq!(AdminManifestTwo2::new(zero, gov).err(), Some(ZeroField));
	let mut ns = base.clone();
	ns.namespace.table = [7; 16];
	assert_eq!(AdminManifestTwo2::new(ns, gov).err(), Some(Namespace));
	let id_reuse = GovernanceRole2::new(base.admin_id, gov_key).unwrap();
	assert_eq!(
		AdminManifestTwo2::new(base.clone(), id_reuse).err(),
		Some(RoleReuse)
	);
	let key_reuse = GovernanceRole2::new([0x6c; 16], base.admin_key).unwrap();
	assert_eq!(
		AdminManifestTwo2::new(base.clone(), key_reuse).err(),
		Some(RoleReuse)
	);
	let mut ns_and_reuse = base.clone();
	ns_and_reuse.namespace.table = [7; 16];
	assert_eq!(
		AdminManifestTwo2::new(ns_and_reuse, key_reuse).err(),
		Some(Namespace)
	);
}

/// The profile-1 API is unchanged: same lengths, codec, digest and refusals.
#[test]
fn profile_one_api_and_goldens_are_unchanged() {
	assert_eq!(ADMIN_MANIFEST_BYTES2, 137);
	assert_eq!(ADMIN_MANIFEST_P2_BYTES2, 189);
	for name in ["raw-tenant5", "table-tenant0"] {
		let b = bytes(name);
		let m = AdminManifest2::decode(&b).unwrap();
		assert_eq!(m.encode().unwrap().to_vec(), b);
		assert_eq!(manifest_profile2(&b), Ok(ManifestProfile2::One));
		let fixed: [u8; 137] = b.clone().try_into().unwrap();
		assert_eq!(
			admin_manifest_digest2(&fixed).to_vec(),
			hex(valid(name)["pin"].as_str().unwrap())
		);
	}
	// The profile-1 decoder still refuses every profile-2 input with its old error.
	let (raw, _, _) = p2_valid("raw");
	assert_eq!(AdminManifest2::decode(&raw).err(), Some(Length));
	assert_eq!(
		AdminManifest2::decode(&raw[..137]).err(),
		Some(UnsupportedProfile)
	);
}
