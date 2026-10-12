//! M3: the {137, 189} governed marker through the reducer (contract r1 §5, as required by FINAL
//! 01a12e44). On a coherent profile-2 governed root, an otherwise eligible Reserve is a consumed
//! Denied(18) without its exact Admission permission, reserves once the permission is active, and
//! is Denied(19) at the revocation coordinate after revocation; every step validates and
//! round-trips. Fixture cut: `manifest_prime`'s Admission fields with the TEST administrator seed
//! 0x5A (as `admin_command_reference.py`) and a TEST governance role, built through the published
//! profile-2 constructors; the root, tuple and token builders are test-only FIXTURES, and the
//! selector/authority/lineage/Reserve commands are the independent `reserve_vectors.json`. The
//! profile-1 path stays `admission_reserve` (control).
use super::profile_two_binding::{Chain, chain_of};
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifest2, AdminManifestTwo2, GovernanceRole2,
};
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::admin_statement::*;
use crate::storage::mutation_visibility::root2::admission_registry::encode_permission_key2;
use crate::storage::mutation_visibility::root2::incremental::Derived2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::tuple_manifest_len2;
use ed25519_dalek::{Signer, SigningKey};

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn sv() -> serde_json::Value {
	serde_json::from_str(include_str!("statement_vectors.json")).unwrap()
}
fn rv() -> serde_json::Value {
	serde_json::from_str(include_str!("reserve_vectors.json")).unwrap()
}
const ADMIN_SEED: u8 = 0x5A;
/// `manifest_prime` with the TEST admin key, upgraded to profile 2 with a TEST governance role.
fn manifest_two() -> Vec<u8> {
	let mut base =
		AdminManifest2::decode(&unhex(sv()["manifest_prime"].as_str().unwrap())).unwrap();
	base.admin_key = SigningKey::from_bytes(&[ADMIN_SEED; 32])
		.verifying_key()
		.to_bytes();
	let role = GovernanceRole2::new(
		[0x6C; 16],
		SigningKey::from_bytes(&[0x6B; 32])
			.verifying_key()
			.to_bytes(),
	)
	.unwrap();
	AdminManifestTwo2::new(base, role)
		.unwrap()
		.encode()
		.unwrap()
		.to_vec()
}
fn governed_two() -> Chain {
	let c = chain_of(&manifest_two(), 189);
	assert_eq!(tuple_manifest_len2(&c.tuple), 189);
	c
}
fn command(name: &str) -> (Command2, [u8; 16], [u8; 16]) {
	let b = unhex(rv()["commands"][name].as_str().unwrap());
	let (g, c, cmd) = Command2::decode(&b).unwrap();
	assert_eq!(cmd.encode(g, c).unwrap(), b);
	(cmd, g, c)
}
fn claim_of(c: &Command2) -> ClaimProjection2 {
	let Command2::Reserve(x) = c else {
		panic!("reserve")
	};
	x.clone()
}
/// The exact permission key an Admission check derives for this claim.
fn key_of(claim: &ClaimProjection2) -> PermissionKey2 {
	let id = claim.manifest.identity;
	PermissionKey2 {
		authority: claim.authority,
		namespace: AdmissionNamespace2 {
			generation: id.generation,
			tenant: id.tenant,
			scope: id.scope,
			table: id.table,
			schema: id.schema_generation,
		},
	}
}
/// An Admission statement for `key` in the installed administrator's context, signed with the
/// TEST Admission key over the published preimage.
fn statement(admin: &InstalledAdmin2, action: AdminAction2, key: &PermissionKey2) -> Command2 {
	let ctx = admin_context2(admin);
	let unsigned = encode_admin_statement2(action, key, &ctx, &[0; 64]).unwrap();
	let preimage = admin_statement_preimage2(&unsigned).unwrap();
	let signature = SigningKey::from_bytes(&[ADMIN_SEED; 32])
		.sign(&preimage)
		.to_bytes();
	Command2::AdmissionPermission(encode_admin_statement2(action, key, &ctx, &signature).unwrap())
}
type Permissions = [([u8; 73], u64, Option<u64>)];
type Parents = [([u8; 73], u64)];
/// The `admission_reserve` step checks: the result and its envelope round trip, coverage, exact
/// permission and parent inventories, rebuild/validation/root/authority round trips, the origin
/// preserved, and an untouched prior state.
fn step(
	chain: &Chain,
	prev: &MutationAuthorityState2,
	c: &Command2,
	i: u64,
	expected: &ApplyResult2,
	permissions: &Permissions,
	parents: &Parents,
) -> MutationAuthorityState2 {
	let (g, cl) = {
		let a = chain.any.admission();
		(a.generation, a.cluster)
	};
	let before = encode_authority2(prev).unwrap();
	let e = TrustedEntry2::fixture(g, cl, l(i), c).unwrap();
	let v = VerifiedCommand2::fixture(g, cl, c, &[], 0).unwrap();
	let (next, r) = if matches!(c, Command2::AdmissionPermission(_)) {
		advance_admission2(prev, &chain.tuple, &e, &v, c)
	} else {
		advance_root2(prev, &e, &v, c)
	}
	.unwrap_or_else(|x| panic!("step {i}: {x:?}"));
	assert!(r == *expected, "result at {i}");
	let b = r.encode(g, cl, l(i)).unwrap();
	let (dg, dc, dl, dr) = ApplyResult2::decode(&b).unwrap();
	assert!((dg, dc, dl) == (g, cl, l(i)) && dr == r);
	let (root, origin) = root_and_origin2(&next).unwrap();
	assert_eq!(root.state.covered, l(i));
	assert_eq!(root.state.control_len, 189);
	let got = root
		.state
		.permissions
		.values()
		.map(|p| {
			(
				encode_permission_key2(&p.key).unwrap(),
				p.enrolled.index,
				p.revoked.map(|x| x.index),
			)
		})
		.collect::<Vec<_>>();
	let mut want = permissions.to_vec();
	want.sort();
	assert_eq!(got, want, "permissions at {i}");
	let got = root
		.state
		.parents
		.iter()
		.map(|(k, p)| (*k, p.reservation.index))
		.collect::<Vec<_>>();
	let mut want = parents.to_vec();
	want.sort();
	assert_eq!(got, want, "parents at {i}");
	assert!(root.state.derived == Derived2::build(&root.state).unwrap());
	validate_state(&root.state).unwrap();
	let rb = root.encode().unwrap();
	assert_eq!(UnverifiedRoot2::decode(&rb).unwrap().encode().unwrap(), rb);
	let ab = encode_authority2(&next).unwrap();
	assert_eq!(
		UnverifiedAuthority2::decode(&ab).unwrap().encode().unwrap(),
		ab
	);
	let first = root_and_origin2(&chain.rooted).unwrap().1;
	assert!(
		(
			origin.genesis,
			origin.bootstrap_fingerprint,
			origin.bootstrap_command
		) == (
			first.genesis,
			first.bootstrap_fingerprint,
			first.bootstrap_command
		),
		"origin preserved at {i}"
	);
	assert_eq!(
		encode_authority2(prev).unwrap(),
		before,
		"prior state untouched at {i}"
	);
	next
}
fn denied(reason: u16, reference: Option<u64>) -> ApplyResult2 {
	ApplyResult2::Denied {
		reason,
		reference: reference.map(l),
	}
}
fn permission(key: [u8; 73], outcome: u8, enrolled: u64, revoked: Option<u64>) -> ApplyResult2 {
	ApplyResult2::Permission {
		outcome,
		key,
		enrolled: l(enrolled),
		revoked: revoked.map(l),
	}
}
fn reserved(c: &Command2, i: u64) -> ApplyResult2 {
	let claim = claim_of(c);
	ApplyResult2::Parent {
		record: ParentRecord2 {
			held_bytes: claim.held_bytes().unwrap(),
			claim,
			reservation: l(i),
			terminal: None,
		},
		new_effect: false,
	}
}

#[test]
fn a_profile_two_governed_reserve_requires_an_active_exact_permission() {
	let chain = governed_two();
	let admin = install_admin2(&chain.rooted, &chain.tuple).unwrap();
	let (reserve, g, c) = command("reserve");
	let a = chain.any.admission();
	assert_eq!(
		(g, c),
		(a.generation, a.cluster),
		"the vectors bind this root"
	);
	let claim = claim_of(&reserve);
	let k = encode_permission_key2(&key_of(&claim)).unwrap();
	let id = claim.manifest.identity.encode().unwrap();
	// Selector, authority and lineage Grant at 3-5 (registered/granted, as in admission_reserve).
	let advance = |prev: &MutationAuthorityState2, name: &str, i: u64| {
		let (cmd, _, _) = command(name);
		let e = TrustedEntry2::fixture(g, c, l(i), &cmd).unwrap();
		let v = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
		let next = advance_root2(prev, &e, &v, &cmd).unwrap().0;
		validate_state(&root_and_origin2(&next).unwrap().0.state).unwrap();
		next
	};
	let s = advance(&chain.rooted, "selector", 3);
	let s = advance(&s, "authority", 4);
	let prerequisites = advance(&s, "grant", 5);
	// No permission: a consumed Denied(18); no parent.
	step(
		&chain,
		&prerequisites,
		&reserve,
		6,
		&denied(18, None),
		&[],
		&[],
	);
	// The exact permission, granted by the Admission key on this profile-2 root, then the Reserve.
	let granted = step(
		&chain,
		&prerequisites,
		&statement(&admin, AdminAction2::Grant, &key_of(&claim)),
		6,
		&permission(k, 1, 6, None),
		&[(k, 6, None)],
		&[],
	);
	step(
		&chain,
		&granted,
		&reserve,
		7,
		&reserved(&reserve, 7),
		&[(k, 6, None)],
		&[(id, 7)],
	);
	// Revoked at 7: Denied(19) at the revocation coordinate; no parent.
	let revoked = step(
		&chain,
		&granted,
		&statement(&admin, AdminAction2::Revoke, &key_of(&claim)),
		7,
		&permission(k, 3, 6, Some(7)),
		&[(k, 6, Some(7))],
		&[],
	);
	step(
		&chain,
		&revoked,
		&reserve,
		8,
		&denied(19, Some(7)),
		&[(k, 6, Some(7))],
		&[],
	);
}
