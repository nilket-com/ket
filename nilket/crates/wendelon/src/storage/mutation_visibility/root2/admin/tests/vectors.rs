use super::*;
use crate::storage::mutation_visibility::MutationScope;

fn from_fields(f: &serde_json::Value) -> AdminManifest2 {
	let h = |k: &str| hex(f[k].as_str().unwrap());
	AdminManifest2 {
		generation: h("generation").try_into().unwrap(),
		cluster: h("cluster").try_into().unwrap(),
		initial_epoch: f["epoch"].as_u64().unwrap(),
		admin_id: h("admin_id").try_into().unwrap(),
		admin_key: h("admin_key").try_into().unwrap(),
		namespace: AdminNamespace2 {
			tenant: f["tenant"].as_u64().unwrap(),
			scope: match f["scope"].as_u64().unwrap() {
				0 => MutationScope::Raw,
				_ => MutationScope::Table,
			},
			table: h("table").try_into().unwrap(),
			schema: f["schema"].as_u64().unwrap(),
		},
	}
}
#[test]
fn independent_vectors_round_trip_and_digest() {
	let v = vectors();
	assert_eq!(v["length"], ADMIN_MANIFEST_BYTES2);
	assert_eq!(v["read_bound"], ADMIN_MANIFEST_READ_BOUND2);
	for name in ["raw-tenant5", "table-tenant0"] {
		let item = valid(name);
		let fields = item["fields"].clone();
		assert_eq!(
			(
				fields["format"].as_u64(),
				fields["profile"].as_u64(),
				fields["actions"].as_u64()
			),
			(Some(1), Some(1), Some(3))
		);
		let literal: [u8; ADMIN_MANIFEST_BYTES2] = bytes(name).try_into().unwrap();
		let expected = from_fields(&fields);
		assert_eq!(expected.encode().unwrap(), literal, "{name}");
		assert_eq!(
			AdminManifest2::decode(&literal).unwrap(),
			expected,
			"{name}"
		);
		let pin: [u8; 32] = hex(item["pin"].as_str().unwrap()).try_into().unwrap();
		assert_eq!(admin_manifest_digest2(&literal), pin, "{name}");
		assert_eq!(check_admin_manifest_digest2(&literal, &pin), Ok(()));
		let mut wrong = pin;
		wrong[31] ^= 1;
		assert_eq!(
			check_admin_manifest_digest2(&literal, &wrong),
			Err(AdminManifestError2::DigestMismatch)
		);
		// Domain separation: neither an undomained nor a foreign-domain hash is the digest.
		use aws_lc_rs::digest::{SHA256, digest};
		assert_ne!(digest(&SHA256, &literal).as_ref(), pin.as_slice());
		let mut foreign = b"VISROOT2-COMMAND\0".to_vec();
		foreign.extend(literal);
		assert_ne!(digest(&SHA256, &foreign).as_ref(), pin.as_slice());
	}
}
#[test]
fn every_single_bit_change_changes_the_digest_and_never_decodes_to_the_same_value() {
	let literal: [u8; ADMIN_MANIFEST_BYTES2] = bytes("table-tenant0").try_into().unwrap();
	let original = AdminManifest2::decode(&literal).unwrap();
	let digest = admin_manifest_digest2(&literal);
	for i in 0..ADMIN_MANIFEST_BYTES2 {
		for bit in 0..8 {
			let mut b = literal;
			b[i] ^= 1 << bit;
			assert_ne!(admin_manifest_digest2(&b), digest);
			if let Ok(m) = AdminManifest2::decode(&b) {
				assert_ne!(m, original, "byte {i} bit {bit}");
				assert_eq!(m.encode().unwrap(), b);
			}
		}
	}
}
