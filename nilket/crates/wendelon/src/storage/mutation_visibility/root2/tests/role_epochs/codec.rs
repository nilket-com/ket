use super::*;
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("vectors.json")).unwrap()
}
fn bytes(v: &serde_json::Value) -> Vec<u8> {
	let s = v.as_str().unwrap();
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
#[test]
fn empty_extension_preserves_v4_bytes_digest_and_usage() {
	let f = F::new();
	let b = f.root().encode().unwrap();
	assert_eq!(&b[..8], b"VISAUT04");
	assert_eq!(f.root().usage().unwrap().history, 0);
	f.roundtrip();
	let v = vectors();
	let b = bytes(&v["state_v4"]);
	assert_eq!(
		super::super::super::UnverifiedRoot2::decode(&b)
			.unwrap()
			.encode()
			.unwrap(),
		b
	);
}
#[test]
fn v5_role_history_roundtrip_and_python_vectors() {
	let v = vectors();
	let b = bytes(&v["state_v5"]);
	assert_eq!(b.len(), 460);
	assert_eq!(
		super::super::super::UnverifiedRoot2::decode(&b)
			.unwrap()
			.encode()
			.unwrap(),
		b
	);
	let mut f = F::new();
	f.rotate(0, 40);
	f.rotate(2, 41);
	f.roundtrip();
}
#[test]
fn v5_empty_extension_is_noncanonical() {
	let v = vectors();
	let old = bytes(&v["state_v4"]);
	let full = bytes(&v["state_v5"]);
	// Complete hash-valid empty history: refusal is not merely truncation.
	let mut body = old[16..old.len() - 32].to_vec();
	body.extend_from_slice(&full[16 + 240..16 + 240 + 96]);
	body.extend_from_slice(&[0; 24]);
	let b =
		super::super::super::codec::envelope_versioned(b"VISAUT05", 5, &body, ROOT_BYTES2).unwrap();
	assert_eq!(
		super::super::super::UnverifiedRoot2::decode(&b).err(),
		Some(RootError2::Codec)
	);
}
#[test]
fn v5_non_profile_three_lengths_refused() {
	use crate::storage::mutation_visibility::root2::{UnverifiedRoot2, role_epochs};
	// Decoded: every non-241 length is refused by the class check first (0 is no class since c2);
	// the role-epoch gate is reached directly below.
	for (len, want) in [
		(0u64, RootError2::ControlClass),
		(240, RootError2::ControlClass),
		(242, RootError2::ControlClass),
	] {
		let mut b = bytes(&vectors()["state_v5"]);
		b[16 + 176..16 + 184].copy_from_slice(&len.to_le_bytes());
		assert_eq!(
			UnverifiedRoot2::decode(&reseal(b)).err(),
			Some(want),
			"{len}"
		);
	}
	// Direct: the role-epoch gate itself refuses each non-241 length on a valid V5 state.
	let mut f = F::new();
	f.rotate(0, 40);
	let state = f.root().state.clone();
	assert_eq!(
		role_epochs::validate(&state, &mut std::collections::BTreeSet::new()),
		Ok(())
	);
	for len in [0, 240, 242] {
		let mut s = state.clone();
		s.control_len = len;
		assert_eq!(
			role_epochs::validate(&s, &mut std::collections::BTreeSet::new()),
			Err(RootError2::RootInvariant),
			"{len}"
		);
	}
}
#[test]
fn v5_counts_bound_before_record_read() {
	let mut b = bytes(&vectors()["state_v5"]);
	b[16 + 240 + 96] = 65;
	assert_eq!(
		super::super::super::UnverifiedRoot2::decode(&reseal(b))
			.err()
			.map(|e| e),
		Some(RootError2::Limit)
	);
}
#[test]
fn v5_keys_strong_global_unique_and_seed_order() {
	for which in [0, 1] {
		let mut b = bytes(&vectors()["state_v5"]);
		let new = 16 + 240 + 99;
		if which == 0 {
			b[new..new + 32].fill(0);
		} else {
			let key = b[16 + 240..16 + 240 + 32].to_vec();
			b[new..new + 32].copy_from_slice(&key);
		}
		assert!(super::super::super::UnverifiedRoot2::decode(&reseal(b)).is_err());
	}
}
#[test]
fn v5_activation_coords_strict_global_and_in_range() {
	for index in [1u64, 2, 4] {
		let mut b = bytes(&vectors()["state_v5"]);
		let index_at = 16 + 240 + 99 + 32 + 12;
		b[index_at..index_at + 8].copy_from_slice(&index.to_le_bytes());
		assert!(super::super::super::UnverifiedRoot2::decode(&reseal(b)).is_err());
	}
}
#[test]
fn v5_usage_mismatch_and_truncation_refused() {
	let b = bytes(&vectors()["state_v5"]);
	for n in [0, 15, 16, 100, b.len() - 1] {
		assert!(super::super::super::UnverifiedRoot2::decode(&b[..n]).is_err());
	}
	let mut bad = b;
	bad[16 + 196..16 + 204].copy_from_slice(&0u64.to_le_bytes());
	assert!(super::super::super::UnverifiedRoot2::decode(&reseal(bad)).is_err());
}
#[test]
fn role_proof_224_offsets_and_independent_preimages() {
	let v = vectors();
	let b = bytes(&v["proof"]);
	assert_eq!(b.len(), 224);
	for i in 0..2 {
		let p = role_command::preimage(&b, [1, 2], i).unwrap();
		assert_eq!(p, bytes(&v["proof_preimages"][i]));
		let k: [u8; 32] = bytes(&v["public_keys"][i]).try_into().unwrap();
		let at = if i == 0 { 86 } else { 160 };
		let sig: [u8; 64] = b[at..at + 64].try_into().unwrap();
		ed25519_dalek::VerifyingKey::from_bytes(&k)
			.unwrap()
			.verify_strict(&p, &ed25519_dalek::Signature::from_bytes(&sig))
			.unwrap();
	}
}
#[test]
fn role_proof_structure_order_and_canonical_absence() {
	let f = F::new();
	let c = F::command(0, 40);
	let b = f.proof(&c);
	for (at, val) in [
		(0, 0),
		(8, 2),
		(10, 1),
		(76, 4),
		(85, 2),
		(150, 0),
		(151, 1),
	] {
		let mut bad = b.clone();
		bad[at] = val;
		if bad == b {
			continue;
		}
		assert!(role_command::parse(&bad).is_err());
	}
	let mut missing = b;
	missing[85] = 0;
	missing[86..150].fill(0);
	assert!(role_command::parse(&missing).is_ok());
	assert!(
		!role_command::verify(
			&f.installed(),
			&missing,
			c.digest(f.f.dep.generation, f.f.dep.cluster).unwrap(),
			[3, 0]
		)
		.unwrap()
	);
}
#[test]
fn role_proof_joint_tuple_domains_resist_mix_and_match() {
	let f = F::new();
	let c = F::command(2, 40);
	let mut b = f.proof(&c);
	let sig = b[86..150].to_vec();
	b[160..224].copy_from_slice(&sig);
	assert!(
		!role_command::verify(
			&f.installed(),
			&b,
			c.digest(f.f.dep.generation, f.f.dep.cluster).unwrap(),
			[1, 2]
		)
		.unwrap()
	);
	assert_ne!(
		role_command::preimage(&b, [1, 2], 0).unwrap(),
		role_command::preimage(&b, [1, 2], 1).unwrap()
	);
}
#[test]
fn epoch_statement_345_context_role_epoch_and_vectors() {
	let v = vectors();
	let b = bytes(&v["statement"]);
	assert_eq!(b.len(), 345);
	assert_eq!(
		epoch_statement_preimage3(&b).unwrap(),
		bytes(&v["statement_preimage"])
	);
	let k: [u8; 32] = bytes(&v["statement_key"]).try_into().unwrap();
	let sig: [u8; 64] = b[281..].try_into().unwrap();
	ed25519_dalek::VerifyingKey::from_bytes(&k)
		.unwrap()
		.verify_strict(
			&bytes(&v["statement_preimage"]),
			&ed25519_dalek::Signature::from_bytes(&sig),
		)
		.unwrap();
	assert_eq!(u64::from_le_bytes(b[273..281].try_into().unwrap()), 1);
}
