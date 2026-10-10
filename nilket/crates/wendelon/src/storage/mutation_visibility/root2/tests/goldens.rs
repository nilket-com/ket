use super::*;
#[test]
fn s1_independent_literal_record_root_command_result_goldens() {
	let j: serde_json::Value = serde_json::from_str(include_str!("goldens.json")).unwrap();
	let mut count = 0;
	for f in j["fixtures"].as_array().unwrap() {
		let name = f["name"].as_str().unwrap();
		let b = fixture(name);
		assert_eq!(b.len(), f["length"].as_u64().unwrap() as usize);
		let kind = f["kind"].as_str().unwrap();
		let encoded = match kind {
			"untrusted_root_envelope" => UnverifiedRoot2::decode(&b).unwrap().encode().unwrap(),
			"command_envelope" => {
				let (g, c, v) = Command2::decode(&b).unwrap();
				v.encode(g, c).unwrap()
			}
			"untrusted_result_envelope" => {
				let (g, c, l, v) = ApplyResult2::decode(&b).unwrap();
				v.encode(g, c, l).unwrap()
			}
			"body" => match name {
				"identity" => crate::storage::mutation_visibility::MutationIdentity::decode(&b)
					.unwrap()
					.encode()
					.unwrap()
					.to_vec(),
				"binding" => read_binding2(&mut Read2::new(&b))
					.unwrap()
					.encode()
					.unwrap()
					.to_vec(),
				"manifest1" => crate::storage::mutation_visibility::ParentManifest::decode(&b)
					.unwrap()
					.encode()
					.unwrap(),
				"signed_lease2" => SignedLease2::decode(&b).unwrap().encode().unwrap(),
				"grant_ref2" => GrantRef2::decode(&b).unwrap().encode().unwrap(),
				"original_grant2" => OriginalGrant2::decode(&b).unwrap().encode().unwrap(),
				"lineage_record_open" | "lineage_record_renewed" => {
					LineageRecord2::decode(&b).unwrap().encode().unwrap()
				}
				"replica_plan_rf3" => ReplicaPlan2::decode(&b).unwrap().encode().unwrap(),
				"claim_projection2" => ClaimProjection2::decode(&b).unwrap().encode().unwrap(),
				"holder_attestation1" => HolderAttestation2::decode(&b).unwrap().encode().unwrap(),
				"parent_reserved" | "parent_accepted" | "parent_refused_cut" => {
					ParentRecord2::decode(&b).unwrap().encode().unwrap()
				}
				"selector2" => Selector2::decode(&b).unwrap().encode().unwrap(),
				"authority_registration2" => AuthorityRegistration2::decode(&b)
					.unwrap()
					.encode()
					.unwrap(),
				"epoch_cut2" => EpochCut2::decode(&b).unwrap().encode().unwrap(),
				_ => panic!("uncovered golden {name}"),
			},
			_ => panic!(),
		};
		assert_eq!(encoded, b, "{name}");
		count += 1;
	}
	assert_eq!(count, 50);
}
#[test]
fn s1_truncation_every_byte_and_unknown_formats_are_refused() {
	for name in ["root_reserved", "command_reserve", "result_new_accepted"] {
		let b = fixture(name);
		for n in 0..b.len() {
			assert!(
				match name {
					"root_reserved" => UnverifiedRoot2::decode(&b[..n]).is_err(),
					"command_reserve" => Command2::decode(&b[..n]).is_err(),
					_ => ApplyResult2::decode(&b[..n]).is_err(),
				},
				"{name} truncated at {n}"
			)
		}
	}
	for offset in [0, 8, 10, 11] {
		let mut b = fixture("root_reserved");
		b[offset] ^= 1;
		assert!(UnverifiedRoot2::decode(&b).is_err())
	}
}
#[test]
fn s1_declared_counts_and_counters_are_checked_before_trust() {
	let b = fixture("root_reserved");
	for offset in [184, 188, 196, 204, 212] {
		let mut payload = b[16..b.len() - 32].to_vec();
		payload[offset] ^= 1;
		let b = envelope_versioned(b"VISAUT03", 3, &payload, ROOT_BYTES2).unwrap();
		assert!(UnverifiedRoot2::decode(&b).is_err(), "{offset}")
	}
	let mut payload = b[16..b.len() - 32].to_vec();
	payload[212..216].copy_from_slice(&u32::MAX.to_le_bytes());
	assert!(
		UnverifiedRoot2::decode(
			&envelope_versioned(b"VISAUT03", 3, &payload, ROOT_BYTES2).unwrap()
		)
		.is_err()
	)
}
#[test]
fn s1_opaque_debug_does_not_print_keys_or_canonical_bytes() {
	let c = claim();
	let s = format!("{c:?} {:?} {:?}", original(), root("root_reserved"));
	assert!(s.contains("redacted"));
	assert!(!s.contains("766"));
	assert!(!s.contains("[17"));
}
