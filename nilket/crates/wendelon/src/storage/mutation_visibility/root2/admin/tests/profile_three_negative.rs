//! Profile-3 ports of the `negative.rs` refusals: each probe on the 241-byte manifest reaches the
//! same decode, encode or constructor branch the profile-1 probe reaches, and pins its typed error.
//! Inputs are the independent `profile_three_vectors.json` (the `valid` rows, and the 13 negative
//! keys as the `key_*_Admin` error rows carry them).
use super::*;
use crate::storage::mutation_visibility::root2::admin::errors::ManifestRole3;
use AdminManifestError2::*;

fn p3() -> serde_json::Value {
	serde_json::from_str(include_str!("profile_three_vectors.json")).unwrap()
}
fn valid3(name: &str) -> Vec<u8> {
	let v = p3();
	let row = v["valid"]
		.as_array()
		.unwrap()
		.iter()
		.find(|r| r["name"] == name)
		.unwrap()
		.clone();
	hex(row["bytes"].as_str().unwrap())
}
fn decode_unchanged(b: &[u8]) -> Result<AdminManifestThree3, AdminManifestError2> {
	let before = b.to_vec();
	let r = AdminManifestThree3::decode(b);
	assert_eq!(b, &before[..]);
	// The dispatching decoder refuses the same bytes with the same error.
	assert_eq!(
		AnyAdminManifest2::decode(b).err(),
		r.as_ref().err().copied()
	);
	r
}
fn with(name: &str, at: usize, bytes_at: &[u8]) -> Vec<u8> {
	let mut b = valid3(name);
	b[at..at + bytes_at.len()].copy_from_slice(bytes_at);
	b
}
#[test]
fn lengths_and_trailing_bytes() {
	let good = valid3("raw");
	assert!(decode_unchanged(&good).is_ok());
	assert_eq!(decode_unchanged(&[]).err(), Some(Length));
	assert_eq!(decode_unchanged(&good[..240]).err(), Some(Length));
	let mut trailing = good.clone();
	trailing.push(0);
	assert_eq!(decode_unchanged(&trailing).err(), Some(Length));
	let mut at_bound = good.clone();
	at_bound.resize(ADMIN_MANIFEST_READ_BOUND2, 0);
	assert_eq!(decode_unchanged(&at_bound).err(), Some(Length));
	at_bound.push(0);
	assert_eq!(decode_unchanged(&at_bound).err(), Some(OverReadBound));
}
#[test]
fn header_and_zero_fields() {
	assert_eq!(
		decode_unchanged(&with("raw", 0, b"VISADM3\0")).err(),
		Some(Magic)
	);
	assert_eq!(
		decode_unchanged(&with("raw", 8, &2u16.to_le_bytes())).err(),
		Some(UnsupportedFormat)
	);
	assert_eq!(
		decode_unchanged(&with("raw", 10, &4u16.to_le_bytes())).err(),
		Some(UnsupportedProfile)
	);
	// Generation, cluster, initial epoch (the epoch-0 refusal) and administrator id.
	for (at, width) in [(12, 16), (28, 16), (44, 8), (52, 16)] {
		assert_eq!(
			decode_unchanged(&with("raw", at, &vec![0; width])).err(),
			Some(ZeroField),
			"offset {at}"
		);
	}
	// Zero fields precede action and namespace checks.
	let mut both = with("raw", 12, &[0; 16]);
	both[100..104].copy_from_slice(&0u32.to_le_bytes());
	both[112] = 2;
	assert_eq!(decode_unchanged(&both).err(), Some(ZeroField));
}
#[test]
fn admin_action_bits_are_exactly_grant_and_revoke() {
	for bits in [0u32, 1, 2, 7, 0x8000_0003] {
		assert_eq!(
			decode_unchanged(&with("raw", 100, &bits.to_le_bytes())).err(),
			Some(ActionThree(ManifestRole3::Admin)),
			"{bits:#x}"
		);
	}
}
#[test]
fn namespace_rules() {
	// An unknown scope byte, then each Raw and Table shape rule (the shared base check).
	assert_eq!(
		decode_unchanged(&with("raw", 112, &[2])).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("raw", 113, &[1; 16])).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("raw", 129, &1u64.to_le_bytes())).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("table", 113, &[0; 16])).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("table", 129, &0u64.to_le_bytes())).err(),
		Some(Namespace)
	);
	// Raw with table bytes and schema both set is still Raw-invalid, not reinterpreted as Table.
	let mut raw = with("raw", 113, &[1; 16]);
	raw[129..137].copy_from_slice(&1u64.to_le_bytes());
	assert_eq!(decode_unchanged(&raw).err(), Some(Namespace));
}
/// The 13 independent negative keys, read from the `key_*_Admin` error rows (bytes 68..100).
fn negative_keys() -> Vec<[u8; 32]> {
	let v = p3();
	let keys: Vec<[u8; 32]> = v["errors"]
		.as_array()
		.unwrap()
		.iter()
		.filter(|r| r["error"] == "Key:Admin")
		.map(|r| {
			hex(r["bytes"].as_str().unwrap())[68..100]
				.try_into()
				.unwrap()
		})
		.collect();
	assert_eq!(keys.len(), 13);
	keys
}
#[test]
fn every_independent_negative_key_is_refused_by_decode_encode_and_the_role_constructors() {
	let good = AdminManifestThree3::decode(&valid3("raw")).unwrap();
	let admin = ManifestRole3::Admin;
	for key in negative_keys() {
		assert_eq!(check_enrollable_admin_key2(&key), Err(NonCanonicalKey));
		assert_eq!(
			decode_unchanged(&with("raw", 68, &key)).err(),
			Some(KeyThree(admin))
		);
		let mut base = good.base().clone();
		base.admin_key = key;
		assert_eq!(
			AdminManifestThree3::new(base, good.governance(), good.recovery()).err(),
			Some(KeyThree(admin))
		);
		// `GovernanceRole2::new`: a non-enrollable key, after a non-zero id.
		assert_eq!(
			GovernanceRole2::new(good.governance().id(), key).err(),
			Some(NonCanonicalKey)
		);
	}
	assert_eq!(check_enrollable_admin_key2(&good.base().admin_key), Ok(()));
}
#[test]
fn constructors_refuse_values_decode_would_refuse() {
	let good = AdminManifestThree3::decode(&valid3("table")).unwrap();
	let mut zero = good.base().clone();
	zero.initial_epoch = 0;
	assert_eq!(
		AdminManifestThree3::new(zero, good.governance(), good.recovery()).err(),
		Some(ZeroField)
	);
	let mut raw_with_table = good.base().clone();
	raw_with_table.namespace.scope = crate::storage::mutation_visibility::MutationScope::Raw;
	assert_eq!(
		AdminManifestThree3::new(raw_with_table, good.governance(), good.recovery()).err(),
		Some(Namespace)
	);
	// `GovernanceRole2::new`: a zero id before the key.
	assert_eq!(
		GovernanceRole2::new([0; 16], [0; 32]).err(),
		Some(ZeroField)
	);
	assert!(GovernanceRole2::new(good.governance().id(), good.governance().key()).is_ok());
}
