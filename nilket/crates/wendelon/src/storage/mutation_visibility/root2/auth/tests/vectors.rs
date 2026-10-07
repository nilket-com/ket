use super::*;
use ed25519_dalek::{Signature, VerifyingKey};

#[test]
fn literal_preimages_and_signatures_match_independent_oracle() {
	let c = claim();
	let h = holder();
	let cluster = cluster();
	assert_eq!(
		claim_mac_preimage2(&c, cluster).unwrap(),
		bytes("claim_mac_preimage")
	);
	assert_eq!(
		admission_preimage2(&c, cluster).unwrap(),
		bytes("admission_preimage")
	);
	assert_eq!(
		holder_preimage2(&c, cluster, &h).unwrap(),
		bytes("holder_preimage")
	);
	assert_eq!(
		verify_admission_signature2(&c, cluster, &public_key()),
		Ok(())
	);
	assert_eq!(
		verify_holder_signature2(&c, cluster, &h, &public_key()),
		Ok(())
	);
	assert_eq!(c.digest().unwrap().as_slice(), bytes("claim_digest"));
	assert_eq!(
		super::super::super::durable::child_binding2(&c, cluster, h.reservation, 0)
			.unwrap()
			.as_slice(),
		bytes("child_binding")
	);
}
#[test]
fn published_primitives_pin_provider_math_without_authorization() {
	let kat: serde_json::Value = serde_json::from_str(include_str!("primitive_kats.json")).unwrap();
	let b = &kat["blake3"];
	let key: [u8; 32] = b["key_ascii"]
		.as_str()
		.unwrap()
		.as_bytes()
		.try_into()
		.unwrap();
	assert_eq!(
		blake3::keyed_hash(&key, &[]).as_bytes().as_slice(),
		hex(b["output32"].as_str().unwrap())
	);
	let e = &kat["ed25519"];
	let key: [u8; 32] = hex(e["public_key"].as_str().unwrap()).try_into().unwrap();
	let sig: [u8; 64] = hex(e["signature"].as_str().unwrap()).try_into().unwrap();
	VerifyingKey::from_bytes(&key)
		.unwrap()
		.verify_strict(&[], &Signature::from_bytes(&sig))
		.unwrap();
	let c = claim();
	assert_eq!(
		blake3::keyed_hash(
			&bytes("public_test_proof_key").try_into().unwrap(),
			&claim_mac_preimage2(&c, cluster()).unwrap()
		)
		.as_bytes()
		.as_slice(),
		bytes("claim_mac")
	);
}
#[test]
fn zero_only_own_fields_preserves_original_signed_lease() {
	let mut c = claim();
	let cluster = cluster();
	let mac = claim_mac_preimage2(&c, cluster).unwrap();
	let admission = admission_preimage2(&c, cluster).unwrap();
	c.signature = [0xfe; 64];
	assert_eq!(claim_mac_preimage2(&c, cluster).unwrap(), mac);
	assert_eq!(admission_preimage2(&c, cluster).unwrap(), admission);
	c.claim_commitment = [0xed; 32];
	assert_eq!(claim_mac_preimage2(&c, cluster).unwrap(), mac);
	assert_ne!(admission_preimage2(&c, cluster).unwrap(), admission);
	c.original.lease.signature = [0xab; 64];
	let changed = claim_mac_preimage2(&c, cluster).unwrap();
	assert_ne!(changed, mac);
	let original_offset = 6 + c.manifest.encode().unwrap().len() + 32;
	let lease_sig_offset = original_offset + 32 + 72;
	assert_eq!(
		&changed[99 + lease_sig_offset..99 + lease_sig_offset + 64],
		&[0xab; 64]
	);
	let mut h = holder();
	let c = claim();
	let pre = holder_preimage2(&c, cluster, &h).unwrap();
	h.signature = [0xef; 64];
	assert_eq!(holder_preimage2(&c, cluster, &h).unwrap(), pre);
	assert_eq!(
		&pre[129..pre.len() - 64],
		&h.encode().unwrap()[..h.encode().unwrap().len() - 64]
	);
}
#[test]
fn math_success_is_not_committed_original_or_capability_authorization() {
	// Independent fixture deliberately retains an unsigned original lease and placeholder commitments.
	// Successful math returns unit only. No runtime trusted producer can consume it as authority.
	let c = claim();
	assert_eq!(c.original.lease.signature, [0; 64]);
	assert_eq!(
		verify_admission_signature2(&c, cluster(), &public_key()),
		Ok(())
	);
	assert_eq!(
		verify_holder_signature2(&c, cluster(), &holder(), &public_key()),
		Ok(())
	);
}

#[test]
fn maximum_valid_shapes_remain_bounded() {
	use super::super::super::{ChildAck2, VersionStamp2};
	let mut c = claim();
	let child = c.manifest.children[0].clone();
	let plan = c.plans[0].clone();
	let hlc = c.stamps[0].hlc;
	c.manifest.children.clear();
	c.plans.clear();
	c.stamps.clear();
	for i in 0..256u32 {
		let mut ch = child.clone();
		ch.ordinal = i;
		ch.versions = vec![child.versions[0].clone(); 2];
		for row in 0..2u32 {
			ch.versions[row as usize].ordinal = row;
			ch.versions[row as usize].key_token[..4].copy_from_slice(&(i * 2 + row).to_le_bytes());
			c.stamps.push(VersionStamp2 { child: i, row, hlc });
		}
		c.manifest.children.push(ch);
		let mut p = plan.clone();
		p.child = i;
		c.plans.push(p);
	}
	let body = c.encode().unwrap();
	assert_eq!(
		claim_mac_preimage2(&c, cluster()).unwrap().len(),
		body.len() + 99
	);
	assert_eq!(
		admission_preimage2(&c, cluster()).unwrap().len(),
		body.len() + 54
	);
	let mut h = holder();
	h.claim_digest = c.digest().unwrap();
	h.children = c
		.plans
		.iter()
		.map(|p| ChildAck2 {
			child: p.child,
			durable_binding: [0x12; 32],
			nodes: p.nodes.clone(),
		})
		.collect();
	let body = h.encode().unwrap();
	assert_eq!(
		holder_preimage2(&c, cluster(), &h).unwrap().len(),
		body.len() + 129
	);
	assert!(body.len() <= super::super::super::ATTESTATION_BYTES2);
}
