use super::*;
use crate::storage::mutation_visibility::root2::lease_payload::*;
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("governance_vectors.json")).unwrap()
}
fn bytes(field: &str) -> Vec<u8> {
	unhex(vectors()[field].as_str().unwrap())
}
fn lease() -> SignedLease2 {
	let v = vectors();
	let f: Vec<u64> = v["lease_fields"]
		.as_array()
		.unwrap()
		.iter()
		.map(|n| n.as_u64().unwrap())
		.collect();
	SignedLease2 {
		epoch: f[0],
		holder: f[1] as u32,
		range_start: f[2],
		range_end: f[3],
		start: Hlc2 {
			physical_ms: f[4],
			logical: f[5] as u32,
			node: f[6] as u32,
		},
		expiry: Hlc2 {
			physical_ms: f[7],
			logical: f[8] as u32,
			node: f[9] as u32,
		},
		min_commit_index: f[10],
		issuer: f[11] as u32,
		signature: bytes("lease_signature").try_into().unwrap(),
	}
}
#[test]
#[cfg(target_endian = "little")]
fn exact_runtime_prefix_offsets_and_independent_signature() {
	use crate::schema::{HybridTimestamp, LeaseDescriptor, NodeId};
	let l = lease();
	let descriptor = LeaseDescriptor {
		epoch: l.epoch,
		holder: NodeId {
			id: l.holder,
			_reserved: 0,
		},
		range_start: l.range_start,
		range_end: l.range_end,
		lease_start: HybridTimestamp {
			physical_ms: l.start.physical_ms,
			logical: l.start.logical,
			node_id: l.start.node,
		},
		lease_expiry: HybridTimestamp {
			physical_ms: l.expiry.physical_ms,
			logical: l.expiry.logical,
			node_id: l.expiry.node,
		},
		min_commit_index: l.min_commit_index,
		issuer: NodeId {
			id: l.issuer,
			_reserved: 0,
		},
		signature: l.signature,
	};
	assert_eq!(std::mem::size_of::<LeaseDescriptor>(), 144);
	assert_eq!(LEASE_PAYLOAD_BYTES2, 80);
	let b = lease_payload2(&l).unwrap();
	assert_eq!(b.to_vec(), bytes("lease_payload"));
	assert_eq!(
		b.as_slice(),
		crate::network::lease_signing::lease_signed_payload(&descriptor)
	);
	assert_eq!(b.as_slice(), &bytemuck::bytes_of(&descriptor)[..80]);
	assert_eq!(&b[12..16], &[0; 4]);
	assert_eq!(&b[76..80], &[0; 4]);
	let v = vectors();
	let ranges = [
		0..8,
		8..12,
		16..24,
		24..32,
		32..40,
		40..44,
		44..48,
		48..56,
		56..60,
		60..64,
		64..72,
		72..76,
	];
	for (i, r) in ranges.into_iter().enumerate() {
		let n = v["lease_fields"][i].as_u64().unwrap();
		assert_ne!(n, 0);
		let mut octets = [0; 8];
		octets[..r.len()].copy_from_slice(&b[r]);
		assert_eq!(u64::from_le_bytes(octets), n);
	}
	assert_eq!(
		verify_issuer_signature2(&bytes("issuer_public").try_into().unwrap(), &l),
		Ok(())
	);
}
#[test]
#[cfg(target_endian = "little")]
fn all_twelve_signed_field_groups_are_bound() {
	let l = lease();
	let key = bytes("issuer_public").try_into().unwrap();
	for i in 0..12 {
		let mut changed = l.clone();
		match i {
			0 => changed.epoch ^= 1,
			1 => changed.holder ^= 1,
			2 => changed.range_start ^= 1,
			3 => changed.range_end ^= 1,
			4 => changed.start.physical_ms ^= 1,
			5 => changed.start.logical ^= 1,
			6 => changed.start.node ^= 1,
			7 => changed.expiry.physical_ms ^= 1,
			8 => changed.expiry.logical ^= 1,
			9 => changed.expiry.node ^= 1,
			10 => changed.min_commit_index ^= 1,
			11 => changed.issuer ^= 1,
			_ => unreachable!(),
		}
		assert_eq!(
			verify_issuer_signature2(&key, &changed),
			Err(LeasePayloadError2::Signature),
			"{i}"
		);
	}
}
#[test]
#[cfg(target_endian = "little")]
fn issuer_key_and_signature_errors_are_distinct() {
	let l = lease();
	let key = bytes("issuer_public").try_into().unwrap();
	assert!(ed25519_dalek::VerifyingKey::from_bytes(&[2; 32]).is_err());
	assert_eq!(
		verify_issuer_signature2(&[2; 32], &l),
		Err(LeasePayloadError2::Key)
	);
	let wrong = ed25519_dalek::SigningKey::from_bytes(&[0xa8; 32])
		.verifying_key()
		.to_bytes();
	assert_eq!(
		verify_issuer_signature2(&wrong, &l),
		Err(LeasePayloadError2::Signature)
	);
	let mut bad = l.clone();
	bad.signature[0] ^= 1;
	assert_eq!(
		verify_issuer_signature2(&key, &bad),
		Err(LeasePayloadError2::Signature)
	);
	bad.signature = bytes("lease_noncanonical_s").try_into().unwrap();
	assert_eq!(
		verify_issuer_signature2(&key, &bad),
		Err(LeasePayloadError2::Signature)
	);
}
#[test]
#[cfg(target_endian = "big")]
fn unsupported_endian_precedes_even_malformed_key() {
	let l = lease();
	assert_eq!(
		lease_payload2(&l),
		Err(LeasePayloadError2::UnsupportedEndian)
	);
	assert_eq!(
		verify_issuer_signature2(&[2; 32], &l),
		Err(LeasePayloadError2::UnsupportedEndian)
	);
}
