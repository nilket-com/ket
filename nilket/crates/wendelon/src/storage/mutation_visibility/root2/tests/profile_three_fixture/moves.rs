//! T-MOVE and T-INLINE for the profile-3 fixtures: every moved or inlined input is byte-identical
//! to what its users read before the move.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::manifest_digest3;
use crate::storage::mutation_visibility::root2::tests::support;

fn hex(b: &[u8]) -> String {
	b.iter().map(|x| format!("{x:02x}")).collect()
}
fn pin(m: &[u8]) -> String {
	hex(&exact_pin(m))
}
fn with_key(base: &AdminManifest2, seed: u8) -> AdminManifest2 {
	let mut b = base.clone();
	b.admin_key = key(seed);
	b
}

/// T-MOVE (ii): each literal manifest is its constructor build, with the recorded pin.
#[test]
fn literal_manifests_are_their_constructor_builds_with_pinned_digests() {
	let [_, g, r] = PRIMARY_SEEDS;
	let primary = build_three(
		with_key(&BASE_B, PRIMARY_SEEDS[0]),
		(PRIMARY_GOVERNANCE_ID, key(g)),
		(PRIMARY_RECOVERY_ID, key(r)),
	);
	let sibling = build_three(
		with_key(&BASE_B, PRIMARY_SEEDS[0]),
		(PRIMARY_GOVERNANCE_ID, key(g)),
		(PRIMARY_RECOVERY_ID, key(SIBLING_RECOVERY_SEED)),
	);
	let [oa, og, or] = OTHER_ROLE_IDS;
	let other = build_three(
		AdminManifest2 {
			generation: OTHER_GENERATION,
			cluster: OTHER_CLUSTER,
			initial_epoch: OTHER_INITIAL_EPOCH,
			admin_id: oa,
			admin_key: key(OTHER_SEEDS[0]),
			namespace: AdminNamespace2 {
				tenant: OTHER_TENANT,
				scope: MutationScope::Raw,
				table: [0; 16],
				schema: 0,
			},
		},
		(og, key(OTHER_SEEDS[1])),
		(or, key(OTHER_SEEDS[2])),
	);
	let [sa, sg, sr] = SEEDED_ROLE_IDS;
	let s1 = build_three(
		AdminManifest2 {
			generation: S1_GENERATION,
			cluster: S1_CLUSTER,
			initial_epoch: S1_INITIAL_EPOCH,
			admin_id: sa,
			admin_key: public(&ADMIN_SEED),
			namespace: AdminNamespace2 {
				tenant: S1_TENANT,
				scope: MutationScope::Raw,
				table: [0; 16],
				schema: 0,
			},
		},
		(sg, public(&GOVERNANCE_SEED)),
		(sr, public(&RECOVERY_SEED)),
	);
	assert_eq!(M3_PRIMARY, primary);
	assert_eq!(M3_SIBLING, sibling);
	assert_eq!(M3_OTHER, other);
	assert_eq!(M3_S1, s1);
	let pins = [
		pin(&M3_PRIMARY),
		pin(&M3_SIBLING),
		pin(&M3_OTHER),
		pin(&M3_S1),
		pin(&M3_KAT),
		pin(&m3_kat_foreign()),
	];
	assert_eq!(
		pins,
		[
			"00462b6fadc4bac28ab59d353106ab8296cc8dd498e57fbb979fa132a5498bdc",
			"1f5bd7a119bc748efab12ff7f342f42dcf4c95a026e09c02403ca6ea2187b729",
			"cdf92250646fc963ff647e940477f3776308257465c106470bfa6134763e234f",
			"00defc774bf27492c007834c61f68c4ec0c21dc683c1490b4307ee7b6be3e4c5",
			"00defc774bf27492c007834c61f68c4ec0c21dc683c1490b4307ee7b6be3e4c5",
			"329e83ea7e8890da9f8c9b33931f49203da0791d05ed0c267c4096ba4173f816",
		]
	);
}

/// The S1 claim namespace that `M3_S1` copies, read from the frozen claim.
#[test]
fn s1_manifest_carries_the_s1_claim_namespace() {
	let id = s1_claim().manifest.identity;
	let m = AdminManifestThree3::decode(&M3_S1).unwrap();
	let a = m.base();
	assert_eq!(
		(
			id.generation,
			id.tenant,
			id.scope,
			id.table,
			id.schema_generation
		),
		(
			a.generation,
			a.namespace.tenant,
			a.namespace.scope,
			a.namespace.table,
			a.namespace.schema
		)
	);
}

/// T-MOVE (ii): the support modules' root, built from `M3_PRIMARY`, is byte-identical to the one
/// they built from manifest "b" before the move; likewise the compromise sibling.
#[test]
fn support_roots_are_unchanged_by_the_move() {
	for (seed, literal) in [(13, M3_PRIMARY), (SIBLING_RECOVERY_SEED, M3_SIBLING)] {
		// `BASE_B` is manifest "b"'s admission part (T-MOVE (i)).
		let mut base = BASE_B.clone();
		base.admin_key = key(11);
		let old = build_three(
			base,
			(PRIMARY_GOVERNANCE_ID, key(12)),
			(PRIMARY_RECOVERY_ID, key(seed)),
		);
		assert_eq!(old, literal);
		let before = Fixture::from_manifest(old.to_vec());
		let after = Fixture::profile_three(&literal);
		assert_eq!(after.manifest, before.manifest);
		assert_eq!(
			encode_authority2(&after.state).unwrap(),
			encode_authority2(&before.state).unwrap()
		);
		assert_eq!(
			tuple_manifest_pin2(&after.pin),
			tuple_manifest_pin2(&before.pin)
		);
		assert_eq!(
			tuple_inventory_pin2(&after.pin),
			tuple_inventory_pin2(&before.pin)
		);
	}
}

/// T-MOVE (vi), re-targeted at b3, which replaces the live S1 parent-lineage goldens with their
/// profile-3 twins. The frozen entries outside the parent lineage still equal their goldens. The
/// three frozen parent-lineage entries are the pre-twin bytes: the claim is the one the pre-twin
/// `root_reserved` (corpus N13) holds, and each twin is its frozen entry under the D-G1 rule (every
/// `Log2` index >= 4 moved by one, with the grant reference and claim digest recomputed).
#[test]
fn frozen_s1_bytes_are_the_pre_twin_goldens() {
	for name in ["command_bootstrap", "command_cover_blank"] {
		assert_eq!(s1_frozen(name), support::fixture(name), "{name}");
	}
	let corpus: serde_json::Value =
		serde_json::from_str(include_str!("../removed_forms.json")).unwrap();
	let pre_twin = corpus["entries"]
		.as_array()
		.unwrap()
		.iter()
		.find(|e| e["id"] == "N13" && e["part"] == "root_reserved")
		.unwrap();
	// The pre-twin root is refused since c2 (ControlClass), so it is read as bytes: it carries the
	// frozen claim's exact encoding.
	let bytes = unhex(pre_twin["hex"].as_str().unwrap());
	let frozen_claim = s1_frozen("claim_projection2");
	assert!(bytes.windows(frozen_claim.len()).any(|w| w == frozen_claim));
	let (g, c) = ([0x11; 16], [0x12; 16]);
	assert!(Command2::Reserve(s1_claim()).encode(g, c).unwrap() == s1_frozen("command_reserve"));
	// The twins.
	let mut claim = s1_claim();
	claim.original.log.index += 1;
	assert!(claim == support::claim());
	assert!(Command2::Reserve(claim.clone()) == support::command("command_reserve"));
	let Command2::Finalize {
		identity,
		reservation,
		mut attestation,
		..
	} = s1_command("command_finalize")
	else {
		panic!("frozen command_finalize is a Finalize")
	};
	assert_eq!(reservation.index, 5, "the pre-twin reservation");
	attestation.reservation.index += 1;
	attestation.grant = claim.original.reference().unwrap();
	attestation.claim_digest = claim.digest().unwrap();
	assert!(
		Command2::Finalize {
			identity,
			digest: claim.digest().unwrap(),
			reservation: attestation.reservation,
			attestation,
		} == support::command("command_finalize")
	);
}

/// T-INLINE: the tuple's manifest digest (the inline 241-byte arm) equals the shared digest of the
/// canonical encoding and the pin the inventory entry carries, for every profile-3 fixture.
#[test]
fn tuple_digest_equals_the_inventory_pin() {
	for m in [
		M3_PRIMARY,
		M3_SIBLING,
		M3_OTHER,
		M3_S1,
		M3_KAT,
		m3_kat_foreign(),
	] {
		let c = chain_of(&m, 241);
		let any = AdminManifestThree3::decode(&m).unwrap();
		let shared = manifest_digest3(&any.encode().unwrap());
		assert_eq!(tuple_manifest_pin2(&c.tuple), shared);
		assert_eq!(tuple_record2(&c.tuple).identity.pin, shared);
		assert_eq!(
			AdminManifestThree3::decode(&m).unwrap().encode().unwrap(),
			m
		);
	}
}

/// `M3_KAT` is the KAT build from `auth/tests/vectors.json` and equals `M3_S1`; the foreign KAT
/// manifest differs from it in the cluster only.
#[test]
fn kat_manifests_are_built_from_the_auth_vectors() {
	assert_eq!(kat_manifest(kat_cluster()), M3_KAT);
	assert_eq!(M3_KAT, M3_S1);
	let foreign = m3_kat_foreign();
	assert_eq!(foreign, kat_manifest(KAT_FOREIGN_CLUSTER));
	let differing: Vec<usize> = (0..241).filter(|&i| foreign[i] != M3_KAT[i]).collect();
	assert_eq!(differing, (28..44).collect::<Vec<_>>());
	assert_eq!(foreign[28..44], KAT_FOREIGN_CLUSTER);
}
