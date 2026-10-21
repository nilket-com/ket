//! VISADS02 authentication on the `M3_PRIME` root (the G1d fixture's admission fields, profile 3),
//! against the independent `p3_` statement vectors.
use super::profile_three_fixture::key as role_key;
use super::profile_three_fixture::{
	PRIME_GOVERNANCE, PRIME_RECOVERY, build_three, exact_pin, tuple_of,
};
use super::*;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin::manifest::AdminManifest2;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::admin_statement::*;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn v() -> serde_json::Value {
	serde_json::from_str(include_str!("statement_vectors.json")).unwrap()
}
fn hex(field: &str) -> Vec<u8> {
	unhex(v()[field].as_str().unwrap())
}
fn d32(field: &str) -> Digest2 {
	hex(field).try_into().unwrap()
}
fn statement(name: &str) -> Vec<u8> {
	unhex(v()[name]["bytes"].as_str().unwrap())
}
fn prime_admission() -> AdminManifest2 {
	AnyAdminManifest2::decode(&hex("p3_manifest"))
		.unwrap()
		.admission()
		.clone()
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
// A root through the existing fixtures: genesis, scanned entries, then the Bootstrap at `index`.
fn rooted(control: Digest2, genesis: Digest2, index: u64) -> MutationAuthorityState2 {
	let m = prime_admission();
	let (g, c) = (m.generation, m.cluster);
	let first = CommittedEntry2::fixture(at(0, 0, 0), genesis, EntryClass2::Membership);
	let mut s = advance_pre_root2(&empty_authority2(), &first).unwrap().0;
	for i in 1..index {
		let scanned = CommittedEntry2::fixture(at(1, 1, i), [i as u8; 32], EntryClass2::NonRoot2);
		s = advance_pre_root2(&s, &scanned).unwrap().0;
	}
	let cmd = Command2::Bootstrap {
		epoch: m.initial_epoch,
		control,
		length: 241,
	};
	let e = TrustedEntry2::fixture(g, c, l(index), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let verified = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
	let ce = CommittedEntry2::fixture(at(1, 1, index), fp, EntryClass2::Bootstrap(d));
	bootstrap2(&s, &ce, &e, &verified, &cmd).unwrap().0
}
fn standard_root() -> MutationAuthorityState2 {
	rooted(d32("p3_manifest_pin"), d32("genesis"), 2)
}
// A coherent Part A tuple for `manifest` whose Bound record is `record`; Part A must accept it.
fn tuple(manifest_bytes: &[u8], record: BootstrapRecord2) -> PinnedBoundTuple2 {
	tuple_of(manifest_bytes, record)
}
fn vector_record() -> BootstrapRecord2 {
	BootstrapRecord2::decode(&hex("p3_record")).unwrap()
}
fn admin() -> InstalledAdmin2 {
	install_admin2(
		&standard_root(),
		&tuple(&hex("p3_manifest"), vector_record()),
	)
	.unwrap()
}
fn key() -> PermissionKey2 {
	let m = prime_admission();
	PermissionKey2 {
		authority: crate::storage::mutation_visibility::root2::types::AuthorityRef2 {
			id: [0x41; 16],
			revision: 1,
		},
		namespace: AdmissionNamespace2 {
			generation: m.generation,
			tenant: m.namespace.tenant,
			scope: m.namespace.scope,
			table: m.namespace.table,
			schema: m.namespace.schema,
		},
	}
}
fn signature_of(bytes: &[u8]) -> [u8; 64] {
	bytes[273..337].try_into().unwrap()
}
fn patched(base: &[u8], at: usize, v: &[u8]) -> Vec<u8> {
	let mut b = base.to_vec();
	b[at..at + v.len()].copy_from_slice(v);
	b
}

#[test]
fn independent_statement_vectors_match_and_authenticate() {
	let state = standard_root();
	// The independently generated snapshot, pins and origin facts match the accepted pipeline.
	let snapshot = hex("p3_snapshot");
	let manifest_pin = d32("p3_manifest_pin");
	let inventory_pin = d32("p3_inventory_pin");
	let vector_tuple = validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&hex("p3_manifest")),
		manifest_pin: Some(&manifest_pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.unwrap();
	let (_, origin) = root_and_origin2(&state).unwrap();
	assert_eq!(origin.genesis, d32("genesis"));
	assert_eq!(origin.bootstrap_command, d32("p3_bootstrap_command"));
	assert_eq!(
		origin.bootstrap_fingerprint,
		d32("p3_bootstrap_fingerprint_stand_in")
	);
	let a = install_admin2(&state, &vector_tuple).unwrap();
	assert_eq!(admin_key2(&a).to_vec(), hex("p3_admin_public"));
	let ctx = admin_context2(&a);
	for (name, action) in [
		("p3_grant", AdminAction2::Grant),
		("p3_revoke", AdminAction2::Revoke),
	] {
		let bytes = statement(name);
		let encoded = encode_admin_statement2(action, &key(), &ctx, &signature_of(&bytes)).unwrap();
		assert_eq!(encoded.to_vec(), bytes, "{name}");
		assert_eq!(
			admin_statement_preimage2(&encoded).unwrap(),
			unhex(v()[name]["preimage"].as_str().unwrap())
		);
		assert_eq!(&encoded[12..200], hex("p3_context").as_slice());
		assert_eq!(&encoded[200..273], hex("p3_key").as_slice());
		let checked = check_admin_statement2(&a.view(), &bytes).unwrap();
		assert_eq!(checked_action2(&checked), action);
		assert!(checked_key2(&checked) == key());
		assert!(checked_context2(&checked) == ctx);
		assert_eq!(format!("{checked:?}"), "CheckedStatement2 { redacted }");
	}
}
#[test]
fn structural_refusals_in_order() {
	use StatementError2 as E;
	let a = admin();
	let good = statement("p3_grant");
	let check = |b: &[u8]| check_admin_statement2(&a.view(), b).err();
	assert_eq!(check(&good[..336]), Some(E::Length));
	for (at, v, want) in [
		(0, &b"VISADS03"[..], E::Magic),
		(8, &2u16.to_le_bytes()[..], E::Version),
		(11, &[1][..], E::Reserved),
		(10, &[0][..], E::Action),
		(10, &[3][..], E::Action),
		(
			12,
			&[0; 16][..],
			E::ZeroField(ZeroField2::ContextGeneration),
		),
		(28, &[0; 16][..], E::ZeroField(ZeroField2::ContextCluster)),
		(
			160,
			&0u64.to_le_bytes()[..],
			E::ZeroField(ZeroField2::ContextEpoch),
		),
		(44, &0u64.to_le_bytes()[..], E::InvalidLog),
		(52, &0u32.to_le_bytes()[..], E::InvalidLog),
		(56, &0u64.to_le_bytes()[..], E::InvalidLog),
		(200, &[0; 16][..], E::ZeroField(ZeroField2::AuthorityId)),
		(
			216,
			&0u64.to_le_bytes()[..],
			E::ZeroField(ZeroField2::AuthorityRevision),
		),
		(
			224,
			&[0; 16][..],
			E::ZeroField(ZeroField2::NamespaceGeneration),
		),
		(248, &[2][..], E::Namespace),
		(249, &[0; 16][..], E::Namespace),
		(265, &0u64.to_le_bytes()[..], E::Namespace),
		(248, &[0][..], E::Namespace),
	] {
		let b = patched(&good, at, v);
		assert_eq!(check(&b), Some(want), "offset {at}");
		let array: [u8; ADMIN_STATEMENT_BYTES2] = b.as_slice().try_into().unwrap();
		assert_eq!(
			admin_statement_preimage2(&array).err(),
			Some(want),
			"offset {at}"
		);
	}
	// The unknown scope tag itself (the step after the shape check): tag 2 with a zero table and
	// schema passes the shape check as Raw and is refused only for the tag. Control: the same
	// bytes with tag 0 (Raw) pass every structural check, and only the later scope check (the
	// administrator's Table namespace) refuses them.
	let zeroed = patched(&good, 249, &[0; 24]);
	let unknown = patched(&zeroed, 248, &[2]);
	let raw = patched(&zeroed, 248, &[0]);
	let fixed = |b: &[u8]| -> [u8; ADMIN_STATEMENT_BYTES2] { b.try_into().unwrap() };
	assert_eq!(check(&unknown), Some(E::Namespace));
	assert_eq!(
		admin_statement_preimage2(&fixed(&unknown)).err(),
		Some(E::Namespace)
	);
	assert!(admin_statement_preimage2(&fixed(&raw)).is_ok());
	assert_eq!(check(&raw), Some(E::Scope));
	// Zero fields outrank the namespace shape.
	let both = patched(&patched(&good, 216, &0u64.to_le_bytes()), 248, &[2]);
	assert_eq!(
		check(&both),
		Some(E::ZeroField(ZeroField2::AuthorityRevision))
	);
	// Encode refuses the same typed inputs; invalid wire tags cannot be typed.
	let ctx = admin_context2(&a);
	let sig = [0u8; 64];
	let mut k = key();
	k.authority.revision = 0;
	assert_eq!(
		encode_admin_statement2(AdminAction2::Grant, &k, &ctx, &sig).err(),
		Some(E::ZeroField(ZeroField2::AuthorityRevision))
	);
	let mut k = key();
	k.namespace.scope = MutationScope::Raw;
	assert_eq!(
		encode_admin_statement2(AdminAction2::Grant, &k, &ctx, &sig).err(),
		Some(E::Namespace)
	);
	let mut c = ctx;
	c.generation = [0; 16];
	assert_eq!(
		encode_admin_statement2(AdminAction2::Grant, &key(), &c, &sig).err(),
		Some(E::ZeroField(ZeroField2::ContextGeneration))
	);
	let mut c = ctx;
	c.installed_at.term = 0;
	assert_eq!(
		encode_admin_statement2(AdminAction2::Grant, &key(), &c, &sig).err(),
		Some(E::InvalidLog)
	);
	// NotCanonical is defensive: every byte is checked or free, so it is not reachable.
}
#[test]
fn context_must_equal_the_installed_context_whole() {
	let a = admin();
	let good = statement("p3_grant");
	for (at, width) in [
		(12, 16),
		(28, 16),
		(44, 8),
		(52, 4),
		(56, 8),
		(64, 32),
		(96, 32),
		(128, 32),
		(160, 8),
		(168, 32),
	] {
		let mut b = good.clone();
		b[at] ^= 0x01;
		b[at + width - 1] |= 0x01;
		assert_eq!(
			check_admin_statement2(&a.view(), &b).err(),
			Some(StatementError2::Context),
			"context field at {at}"
		);
	}
}
#[test]
fn scope_must_equal_the_admin_namespace_exactly() {
	let a = admin();
	let ctx = admin_context2(&a);
	let sig = signature_of(&statement("p3_grant"));
	let raw = AdmissionNamespace2 {
		scope: MutationScope::Raw,
		table: [0; 16],
		schema: 0,
		..key().namespace
	};
	let mut changes = vec![raw];
	let base = key().namespace;
	changes.push(AdmissionNamespace2 {
		generation: [0x33; 16],
		..base
	});
	changes.push(AdmissionNamespace2 {
		tenant: base.tenant + 1,
		..base
	});
	changes.push(AdmissionNamespace2 {
		table: [0x34; 16],
		..base
	});
	changes.push(AdmissionNamespace2 {
		schema: base.schema + 1,
		..base
	});
	for namespace in changes {
		let k = PermissionKey2 { namespace, ..key() };
		let bytes = encode_admin_statement2(AdminAction2::Grant, &k, &ctx, &sig).unwrap();
		assert_eq!(
			check_admin_statement2(&a.view(), &bytes).err(),
			Some(StatementError2::Scope)
		);
	}
}
#[test]
fn signatures_are_strict_and_bound_to_the_admin_key() {
	let a = admin();
	let mut flipped = statement("p3_grant");
	flipped[300] ^= 1;
	assert_eq!(
		check_admin_statement2(&a.view(), &flipped).err(),
		Some(StatementError2::Signature)
	);
	assert_eq!(
		check_admin_statement2(&a.view(), &statement("p3_other_signer")).err(),
		Some(StatementError2::Signature)
	);
}
/// Role-key substitution: the same statement bytes signed, validly, by `M3_PRIME`'s governance or
/// recovery key instead of its administrator key are refused at the signature. The
/// administrator-signed statement is the accepted twin.
#[test]
fn a_statement_signed_by_another_role_key_is_refused() {
	use crate::storage::mutation_visibility::root2::tests::profile_three_fixture::{
		PRIME_GOVERNANCE, PRIME_RECOVERY,
	};
	use ed25519_dalek::{Signer, SigningKey};
	let a = admin();
	let grant = statement("p3_grant");
	assert!(check_admin_statement2(&a.view(), &grant).is_ok());
	let preimage = admin_statement_preimage2(&grant.clone().try_into().unwrap()).unwrap();
	for (role, seed) in [
		("governance", PRIME_GOVERNANCE.1),
		("recovery", PRIME_RECOVERY.1),
	] {
		let signer = SigningKey::from_bytes(&[seed; 32]);
		let signature = signer.sign(&preimage);
		signer
			.verifying_key()
			.verify_strict(&preimage, &signature)
			.expect("a valid signature by the role key");
		let mut by_role = grant.clone();
		by_role[273..].copy_from_slice(&signature.to_bytes());
		assert_eq!(
			check_admin_statement2(&a.view(), &by_role).err(),
			Some(StatementError2::Signature),
			"{role}"
		);
	}
}
#[test]
fn statements_survive_tip_advances_and_do_not_cross_roots() {
	let grant = statement("p3_grant");
	let t = tuple(&hex("p3_manifest"), vector_record());
	let mut state = standard_root();
	let m = prime_admission();
	for (i, authorized) in [(3u64, true), (4, false)] {
		let cmd = Command2::Cover {
			kind: 0,
			external: [i as u8; 32],
		};
		let e = TrustedEntry2::fixture(m.generation, m.cluster, l(i), &cmd).unwrap();
		let mut verified =
			VerifiedCommand2::fixture(m.generation, m.cluster, &cmd, &[], 0).unwrap();
		if !authorized {
			verified = verified.unauthorized();
		}
		state = advance_root2(&state, &e, &verified, &cmd).unwrap().0;
		let a = install_admin2(&state, &t).unwrap();
		assert!(
			check_admin_statement2(&a.view(), &grant).is_ok(),
			"entry {i}"
		);
	}
	// Another root with a different genesis, coherently installed, refuses root A's statement.
	let mut other_genesis = vector_record();
	other_genesis.identity.genesis_fingerprint = [0x77; 32];
	let b = install_admin2(
		&rooted(d32("p3_manifest_pin"), [0x77; 32], 2),
		&tuple(&hex("p3_manifest"), other_genesis),
	)
	.unwrap();
	assert_eq!(
		check_admin_statement2(&b.view(), &grant).err(),
		Some(StatementError2::Context)
	);
	// Another root whose Bootstrap sits at another log position.
	let later = rooted(d32("p3_manifest_pin"), d32("genesis"), 3);
	let (_, origin) = root_and_origin2(&later).unwrap();
	let mut moved = vector_record();
	moved.identity.bootstrap = l(3);
	moved.identity.bootstrap_fingerprint = origin.bootstrap_fingerprint;
	moved.identity.bootstrap_command = origin.bootstrap_command;
	let c = install_admin2(&later, &tuple(&hex("p3_manifest"), moved)).unwrap();
	assert_eq!(
		check_admin_statement2(&c.view(), &grant).err(),
		Some(StatementError2::Context)
	);
	// A coherently installed administrator with a different key commits a different manifest pin,
	// so the whole context differs: Context, not Signature.
	let mut rekeyed = prime_admission();
	rekeyed.admin_key = role_key(0x2F);
	let other = build_three(
		rekeyed,
		(PRIME_GOVERNANCE.0, role_key(PRIME_GOVERNANCE.1)),
		(PRIME_RECOVERY.0, role_key(PRIME_RECOVERY.1)),
	);
	let other_pin = exact_pin(&other);
	let other_root = rooted(other_pin, d32("genesis"), 2);
	let (_, origin) = root_and_origin2(&other_root).unwrap();
	let mut other_record = vector_record();
	other_record.identity.pin = other_pin;
	other_record.identity.bootstrap_fingerprint = origin.bootstrap_fingerprint;
	other_record.identity.bootstrap_command = origin.bootstrap_command;
	let other_key = install_admin2(&other_root, &tuple(&other, other_record)).unwrap();
	assert!(admin_key2(&other_key) != admin_key2(&admin()));
	assert_eq!(
		check_admin_statement2(&other_key.view(), &grant).err(),
		Some(StatementError2::Context)
	);
}
