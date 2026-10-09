use super::*;
use AdminManifestError2::*;

fn decode_unchanged(b: &[u8]) -> Result<AdminManifest2, AdminManifestError2> {
	let before = b.to_vec();
	let r = AdminManifest2::decode(b);
	assert_eq!(b, &before[..]);
	r
}
fn with(name: &str, at: usize, bytes_at: &[u8]) -> Vec<u8> {
	let mut b = bytes(name);
	b[at..at + bytes_at.len()].copy_from_slice(bytes_at);
	b
}
#[test]
fn lengths_and_trailing_bytes() {
	let good = bytes("raw-tenant5");
	assert_eq!(decode_unchanged(&[]).err(), Some(Length));
	assert_eq!(decode_unchanged(&good[..136]).err(), Some(Length));
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
		decode_unchanged(&with("raw-tenant5", 0, b"VISADM3\0")).err(),
		Some(Magic)
	);
	assert_eq!(
		decode_unchanged(&with("raw-tenant5", 8, &2u16.to_le_bytes())).err(),
		Some(UnsupportedFormat)
	);
	assert_eq!(
		decode_unchanged(&with("raw-tenant5", 10, &2u16.to_le_bytes())).err(),
		Some(UnsupportedProfile)
	);
	for (at, width) in [(12, 16), (28, 16), (44, 8), (52, 16)] {
		assert_eq!(
			decode_unchanged(&with("raw-tenant5", at, &vec![0; width])).err(),
			Some(ZeroField),
			"offset {at}"
		);
	}
	// Zero fields precede action and namespace checks.
	let mut both = with("raw-tenant5", 12, &[0; 16]);
	both[100..104].copy_from_slice(&0u32.to_le_bytes());
	assert_eq!(decode_unchanged(&both).err(), Some(ZeroField));
}
#[test]
fn action_bits_are_exactly_grant_and_revoke() {
	for bits in [0u32, 1, 2, 7, 0x8000_0003] {
		assert_eq!(
			decode_unchanged(&with("raw-tenant5", 100, &bits.to_le_bytes())).err(),
			Some(UnsupportedAction),
			"{bits:#x}"
		);
	}
}
#[test]
fn namespace_rules() {
	assert_eq!(
		decode_unchanged(&with("raw-tenant5", 112, &[2])).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("raw-tenant5", 113, &[1; 16])).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("raw-tenant5", 129, &1u64.to_le_bytes())).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("table-tenant0", 113, &[0; 16])).err(),
		Some(Namespace)
	);
	assert_eq!(
		decode_unchanged(&with("table-tenant0", 129, &0u64.to_le_bytes())).err(),
		Some(Namespace)
	);
	// Raw with table bytes and schema both set is still Raw-invalid, not reinterpreted as Table.
	let mut raw = with("raw-tenant5", 113, &[1; 16]);
	raw[129..137].copy_from_slice(&1u64.to_le_bytes());
	assert_eq!(decode_unchanged(&raw).err(), Some(Namespace));
}
#[test]
fn every_independent_negative_key_is_refused_by_decode_encode_and_enrollment() {
	let good = AdminManifest2::decode(&bytes("raw-tenant5")).unwrap();
	let keys = vectors()["negative_keys"].as_array().unwrap().clone();
	assert_eq!(keys.len(), 13);
	for k in keys {
		let key: [u8; 32] = hex(k["key"].as_str().unwrap()).try_into().unwrap();
		assert_eq!(
			check_enrollable_admin_key2(&key),
			Err(NonCanonicalKey),
			"{}",
			k["name"]
		);
		assert_eq!(
			decode_unchanged(&with("raw-tenant5", 68, &key)).err(),
			Some(NonCanonicalKey),
			"{}",
			k["name"]
		);
		let mut m = good.clone();
		m.admin_key = key;
		assert_eq!(m.encode().err(), Some(NonCanonicalKey), "{}", k["name"]);
	}
	assert_eq!(check_enrollable_admin_key2(&good.admin_key), Ok(()));
}
#[test]
fn encode_refuses_values_decode_would_refuse() {
	let good = AdminManifest2::decode(&bytes("table-tenant0")).unwrap();
	let mut zero = good.clone();
	zero.initial_epoch = 0;
	assert_eq!(zero.encode().err(), Some(ZeroField));
	let mut raw_with_table = good.clone();
	raw_with_table.namespace.scope = crate::storage::mutation_visibility::MutationScope::Raw;
	assert_eq!(raw_with_table.encode().err(), Some(Namespace));
}
