use super::*;
use ed25519_dalek::{Signature, VerifyingKey};

#[test]
fn strict_math_rejects_malformed_and_weak_points_and_wrong_key() {
	let c = claim();
	let cluster = cluster();
	// This literal must actually fail decompression, rather than merely fail the equation.
	let malformed = [2; 32];
	assert!(VerifyingKey::from_bytes(&malformed).is_err());
	assert_eq!(
		verify_admission_signature2(&c, cluster, &malformed),
		Err(AuthError2::CryptoInvalid)
	);
	let weak_a = [0; 32];
	assert!(VerifyingKey::from_bytes(&weak_a).unwrap().is_weak());
	assert_eq!(
		verify_admission_signature2(&c, cluster, &weak_a),
		Err(AuthError2::CryptoInvalid)
	);
	let mut weak_r = c.clone();
	weak_r.signature[..32].fill(0);
	weak_r.signature[0] = 1;
	assert_eq!(
		verify_admission_signature2(&weak_r, cluster, &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
	let mut malformed_r = c.clone();
	malformed_r.signature[..32].copy_from_slice(&malformed);
	assert_eq!(
		verify_admission_signature2(&malformed_r, cluster, &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
	let wrong_key = hex("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c")
		.try_into()
		.unwrap();
	assert_eq!(
		verify_admission_signature2(&c, cluster, &wrong_key),
		Err(AuthError2::CryptoInvalid)
	);
	assert_eq!(
		verify_holder_signature2(&c, cluster, &holder(), &wrong_key),
		Err(AuthError2::CryptoInvalid)
	);
}
#[test]
fn noncanonical_scalar_rejection_pins_effective_no_legacy_feature() {
	// S+L preserves the equation under permissive legacy scalar parsing. This regression
	// rejects that valid-equation alternative, not just an unrelated invalid signature.
	let order = hex("edd3f55c1a631258d69cf7a2def9de1400000000000000000000000000000010");
	let mut c = claim();
	let mut h = holder();
	for sig in [&mut c.signature, &mut h.signature] {
		let mut carry = 0u16;
		for i in 0..32 {
			let n = sig[i + 32] as u16 + order[i] as u16 + carry;
			sig[i + 32] = n as u8;
			carry = n >> 8;
		}
		assert_eq!(carry, 0);
	}
	assert_eq!(
		verify_admission_signature2(&c, cluster(), &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
	// Holder references the original canonical claim, not the modified admission signature.
	assert_eq!(
		verify_holder_signature2(&claim(), cluster(), &h, &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
}
#[test]
fn alternate_public_encoding_is_characterized_not_enrollment() {
	let canonical = {
		let mut a = [0; 32];
		a[0] = 1;
		a
	};
	// y=p+1 is an alternate identity representation permitted by ZIP-215 decompression.
	let alternate: [u8; 32] =
		hex("eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f")
			.try_into()
			.unwrap();
	let a = VerifyingKey::from_bytes(&alternate).unwrap();
	assert_eq!(a.to_bytes(), alternate); // Preserved input, NOT canonical enrollment proof.
	assert_ne!(a.to_bytes(), canonical);
	assert!(a.is_weak());
	assert!(VerifyingKey::from_bytes(&canonical).unwrap().is_weak());
	assert_eq!(
		verify_admission_signature2(&claim(), cluster(), &alternate),
		Err(AuthError2::CryptoInvalid)
	);
}
#[test]
fn domain_and_signature_equation_tampering_reject() {
	let c = claim();
	let h = holder();
	let key = VerifyingKey::from_bytes(&public_key()).unwrap();
	let mut pre = admission_preimage2(&c, cluster()).unwrap();
	pre[0] ^= 1;
	assert!(
		key.verify_strict(&pre, &Signature::from_bytes(&c.signature))
			.is_err()
	);
	assert!(
		key.verify_strict(
			&holder_preimage2(&c, cluster(), &h).unwrap(),
			&Signature::from_bytes(&c.signature)
		)
		.is_err()
	);
	let mut c2 = c.clone();
	c2.signature[0] ^= 1;
	assert_eq!(
		verify_admission_signature2(&c2, cluster(), &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
	let mut h2 = h.clone();
	h2.signature[63] ^= 1;
	assert_eq!(
		verify_holder_signature2(&c, cluster(), &h2, &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
}
#[test]
fn valid_shape_scope_and_bound_fields_cannot_reuse_signature() {
	let c = claim();
	let mutate: &[fn(&mut ClaimProjection2)] = &[
		|c| c.manifest.identity.generation[0] ^= 1,
		|c| c.manifest.identity.tenant += 1,
		|c| {
			c.manifest.identity.scope = crate::storage::mutation_visibility::MutationScope::Table;
			c.manifest.identity.table = [0x99; 16];
			c.manifest.identity.schema_generation = 1;
			c.manifest.operation = crate::storage::mutation_visibility::MutationOperation::Insert;
		},
		|c| c.manifest.identity.caller[0] ^= 1,
		|c| c.manifest.identity.sequence += 1,
		|c| c.manifest.key_id[0] ^= 1,
		|c| c.manifest.key_version += 1,
		|c| c.manifest_commitment[0] ^= 1,
		|c| c.placement[0] ^= 1,
		|c| c.authority.revision += 1,
		|c| c.stamps[0].hlc.logical += 1,
		|c| c.manifest.children[0].versions[0].content_commitment[0] ^= 1,
		|c| c.manifest.children[0].versions[0].key_token[0] ^= 1,
		|c| c.plans[0].nodes[1].incarnation[0] ^= 1,
		|c| c.claim_commitment[0] ^= 1,
		|c| c.original.lease.signature[0] ^= 1,
	];
	for change in mutate {
		let mut other = c.clone();
		change(&mut other);
		assert_eq!(
			verify_admission_signature2(&other, cluster(), &public_key()),
			Err(AuthError2::CryptoInvalid)
		);
	}
	let mut cluster = cluster();
	cluster[0] ^= 1;
	assert_eq!(
		verify_admission_signature2(&c, cluster, &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
	assert_eq!(
		verify_holder_signature2(&c, cluster, &holder(), &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
}
#[test]
fn holder_cross_references_and_fields_reject_without_claiming_quorum() {
	let c = claim();
	let h = holder();
	let wrong: &[fn(&mut HolderAttestation2)] = &[
		|h| h.grant.digest[0] ^= 1,
		|h| h.grant.log.term += 1,
		|h| h.claim_digest[0] ^= 1,
		|h| h.authority.revision += 1,
	];
	for change in wrong {
		let mut other = h.clone();
		change(&mut other);
		assert_eq!(
			holder_preimage2(&c, cluster(), &other),
			Err(AuthError2::ScopeMismatch)
		);
	}
	let changed: &[fn(&mut HolderAttestation2)] = &[
		|h| h.reservation.term += 1,
		|h| h.reservation.node += 1,
		|h| h.reservation.index += 1,
		|h| h.children[0].durable_binding[0] ^= 1,
		|h| h.children[0].nodes[1].incarnation[0] ^= 1,
	];
	for change in changed {
		let mut other = h.clone();
		change(&mut other);
		assert_eq!(
			verify_holder_signature2(&c, cluster(), &other, &public_key()),
			Err(AuthError2::CryptoInvalid)
		);
	}
	let mut duplicate = h.clone();
	duplicate.children[0].nodes[1] = duplicate.children[0].nodes[0];
	assert_eq!(
		holder_preimage2(&c, cluster(), &duplicate),
		Err(AuthError2::Structural)
	);
	let mut partial = h;
	partial.children[0].nodes.pop();
	// Shape remains encodable; full quorum policy deliberately belongs to a later verifier.
	assert!(holder_preimage2(&c, cluster(), &partial).is_ok());
	assert_eq!(
		verify_holder_signature2(&c, cluster(), &partial, &public_key()),
		Err(AuthError2::CryptoInvalid)
	);
}
#[test]
fn bounds_precede_structural_validation_and_zero_scope_refuses() {
	let c = claim();
	assert_eq!(
		claim_mac_preimage2(&c, [0; 16]),
		Err(AuthError2::ScopeMismatch)
	);
	let mut other = c.clone();
	other.manifest.identity.generation = [0; 16];
	assert_eq!(
		admission_preimage2(&other, cluster()),
		Err(AuthError2::ScopeMismatch)
	);
	let mut other = c.clone();
	other.manifest.children = vec![c.manifest.children[0].clone(); 257];
	assert_eq!(
		admission_preimage2(&other, cluster()),
		Err(AuthError2::Limit)
	);
	let mut other = c.clone();
	other.manifest.children[0].versions = vec![c.manifest.children[0].versions[0].clone(); 513];
	assert_eq!(
		claim_mac_preimage2(&other, cluster()),
		Err(AuthError2::Limit)
	);
	let mut other = c.clone();
	other.stamps = vec![c.stamps[0]; 513];
	assert_eq!(
		admission_preimage2(&other, cluster()),
		Err(AuthError2::Limit)
	);
	let mut other = c.clone();
	other.plans = vec![c.plans[0].clone(); 257];
	assert_eq!(
		admission_preimage2(&other, cluster()),
		Err(AuthError2::Limit)
	);
	let mut other = c.clone();
	other.plans[0].nodes = vec![c.plans[0].nodes[0]; 4];
	assert_eq!(
		admission_preimage2(&other, cluster()),
		Err(AuthError2::Limit)
	);
	let mut h = holder();
	h.children = vec![h.children[0].clone(); 257];
	assert_eq!(holder_preimage2(&c, cluster(), &h), Err(AuthError2::Limit));
	let mut h = holder();
	h.children[0].nodes = vec![h.children[0].nodes[0]; 4];
	assert_eq!(holder_preimage2(&c, cluster(), &h), Err(AuthError2::Limit));
}
#[test]
fn checked_arithmetic_and_structural_errors_are_typed() {
	assert_eq!(
		crate::storage::mutation_visibility::root2::auth::preimage::checked_size(
			usize::MAX,
			1,
			1,
			usize::MAX
		),
		Err(AuthError2::Overflow)
	);
	assert_eq!(
		crate::storage::mutation_visibility::root2::auth::preimage::checked_size(
			0,
			usize::MAX,
			2,
			usize::MAX
		),
		Err(AuthError2::Overflow)
	);
	assert_eq!(
		crate::storage::mutation_visibility::root2::auth::preimage::checked_size(1, 1, 1, 1),
		Err(AuthError2::Limit)
	);
	assert_eq!(
		crate::storage::mutation_visibility::root2::auth::preimage::length(usize::MAX),
		Err(AuthError2::Overflow)
	);
	let mut c = claim();
	c.stamps.clear();
	assert_eq!(
		admission_preimage2(&c, cluster()),
		Err(AuthError2::Structural)
	);
	let mut c = claim();
	c.manifest.children[0].versions[0].tombstone = true;
	assert_eq!(
		claim_mac_preimage2(&c, cluster()),
		Err(AuthError2::Structural)
	);
}

#[test]
fn invalid_row_child_hlc_schema_and_plan_shapes_are_structural() {
	let changes: &[fn(&mut ClaimProjection2)] = &[
		|c| c.manifest.identity.schema_generation = 1, // Raw identity cannot claim a table schema.
		|c| c.manifest.children[0].ordinal = 1,
		|c| c.manifest.children[0].versions[0].ordinal = 1,
		|c| c.stamps[0].row = 1,
		|c| c.stamps[0].child = 1,
		|c| c.stamps[0].hlc = c.original.lease.expiry,
		|c| c.plans[0].nodes[0].incarnation[0] ^= 1,
	];
	for change in changes {
		let mut c = claim();
		change(&mut c);
		assert_eq!(
			admission_preimage2(&c, cluster()),
			Err(AuthError2::Structural)
		);
	}
}
