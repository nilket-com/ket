//! `M3_PRIME` and the independent profile-3 statement vectors (`statement_reference.py`): the
//! literal is its constructor build with a pinned digest, and every generated manifest, pin and
//! D-G3 statement equals the Rust build of the same inputs.
use super::*;

fn hex(b: &[u8]) -> String {
	b.iter().map(|x| format!("{x:02x}")).collect()
}
fn sv() -> serde_json::Value {
	serde_json::from_str(include_str!("../statement_vectors.json")).unwrap()
}
fn sv_hex(v: &serde_json::Value) -> Vec<u8> {
	unhex(v.as_str().unwrap())
}

/// `M3_PRIME` is the profile-1 G1d fixture's admission fields, re-keyed and upgraded, with its pin.
#[test]
fn prime_is_its_constructor_build_with_a_pinned_digest() {
	let (governance, recovery) = (PRIME_GOVERNANCE, PRIME_RECOVERY);
	let built = build_three(
		AdminManifest2 {
			generation: [0xa1; 16],
			cluster: [0xc2; 16],
			initial_epoch: 7,
			admin_id: [0x0e; 16],
			admin_key: key(PRIME_ADMIN_SEED),
			namespace: AdminNamespace2 {
				tenant: 0,
				scope: MutationScope::Table,
				table: [0x7a; 16],
				schema: 2,
			},
		},
		(governance.0, key(governance.1)),
		(recovery.0, key(recovery.1)),
	);
	assert_eq!(M3_PRIME, built);
	assert_eq!(
		hex(&exact_pin(&M3_PRIME)),
		"70cddb670e277dbabfda586adca63baf5b2d0b7a87dbb794fb7949600ab52579"
	);
	let v = sv();
	assert_eq!(sv_hex(&v["p3_manifest"]), M3_PRIME);
	assert_eq!(sv_hex(&v["p3_manifest_pin"]), exact_pin(&M3_PRIME));
	assert_eq!(sv_hex(&v["p3_admin_public"]), key(PRIME_ADMIN_SEED));
}

/// D-G3: each generated KAT statement is the Rust signature, by the published administrator seed,
/// of a Grant for the `claim_c2` authority and namespace in the KAT root's installed context.
#[test]
fn kat_statements_are_the_published_seed_signatures_over_the_kat_contexts() {
	let claim = ClaimProjection2::decode(&kat("claim_c2")).unwrap();
	let id = claim.manifest.identity;
	for (name, manifest) in [
		("p3_kat_grant", M3_KAT),
		("p3_kat_foreign_grant", m3_kat_foreign()),
	] {
		let a = AdminManifestThree3::decode(&manifest).unwrap();
		let a = a.base();
		let boot = Command2::Bootstrap {
			epoch: a.initial_epoch,
			control: exact_pin(&manifest),
			length: 241,
		};
		let e = TrustedEntry2::fixture(a.generation, a.cluster, l(2), &boot).unwrap();
		let (_, _, _, fp, d) = e.fields();
		let ctx = AdminContext2 {
			generation: a.generation,
			cluster: a.cluster,
			installed_at: l(2),
			bootstrap_fingerprint: fp,
			bootstrap_command: d,
			genesis: [0x11; 32],
			initial_epoch: claim.original.lease.epoch,
			manifest_pin: exact_pin(&manifest),
		};
		let k = PermissionKey2 {
			authority: claim.authority,
			namespace: AdmissionNamespace2 {
				generation: id.generation,
				tenant: id.tenant,
				scope: id.scope,
				table: id.table,
				schema: id.schema_generation,
			},
		};
		let unsigned = encode_admin_statement2(AdminAction2::Grant, &k, &ctx, &[0; 64]).unwrap();
		let preimage = admin_statement_preimage2(&unsigned).unwrap();
		let signature = SigningKey::from_bytes(&ADMIN_SEED)
			.sign(&preimage)
			.to_bytes();
		let signed = encode_admin_statement2(AdminAction2::Grant, &k, &ctx, &signature).unwrap();
		let v = &sv()[name];
		assert_eq!(sv_hex(&v["bytes"]), signed, "{name}");
		assert_eq!(sv_hex(&v["preimage"]), preimage, "{name}");
		assert_eq!(sv_hex(&v["manifest_pin"]), exact_pin(&manifest), "{name}");
	}
}
