//! The dormant Admission issuer gate, end to end through `advance_admission2`, plus direct boundary
//! cases on `Root2::reduce_admission`. Fixture cut: the G1d-1 rooted state (m1', Bootstrap at
//! (1,1,2), TEST-S1-ENTRY stand-in), EnrollAuthority 0x41 rev 1 at (1,1,3), statements from the
//! committed vectors and the independently re-signed variants in gate_vectors.json.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AdminManifest2;
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::incremental::Derived2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn sv() -> serde_json::Value {
	serde_json::from_str(include_str!("statement_vectors.json")).unwrap()
}
fn gv() -> serde_json::Value {
	serde_json::from_str(include_str!("gate_vectors.json")).unwrap()
}
fn hex(field: &str) -> Vec<u8> {
	unhex(sv()[field].as_str().unwrap())
}
fn d32(field: &str) -> Digest2 {
	hex(field).try_into().unwrap()
}
fn bytes337(v: Vec<u8>) -> [u8; 337] {
	v.try_into().unwrap()
}
fn statement(name: &str) -> Command2 {
	Command2::AdmissionPermission(bytes337(unhex(sv()[name]["bytes"].as_str().unwrap())))
}
fn variant(v: &str) -> Command2 {
	Command2::AdmissionPermission(bytes337(unhex(v)))
}
fn manifest_prime() -> AdminManifest2 {
	AdminManifest2::decode(&hex("manifest_prime")).unwrap()
}
fn gc() -> ([u8; 16], [u8; 16]) {
	let m = manifest_prime();
	(m.generation, m.cluster)
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
// A rooted state: genesis, scanned entries, then Bootstrap at `index`, optionally with a changed
// original Bootstrap fingerprint, for generation/cluster `g`/`c`.
fn rooted_as(
	g: [u8; 16],
	c: [u8; 16],
	genesis: Digest2,
	index: u64,
	changed: bool,
) -> MutationAuthorityState2 {
	let m = manifest_prime();
	let first = CommittedEntry2::fixture(at(0, 0, 0), genesis, EntryClass2::Membership);
	let mut s = advance_pre_root2(&empty_authority2(), &first).unwrap().0;
	for i in 1..index {
		let scanned = CommittedEntry2::fixture(at(1, 1, i), [i as u8; 32], EntryClass2::NonRoot2);
		s = advance_pre_root2(&s, &scanned).unwrap().0;
	}
	let cmd = Command2::Bootstrap {
		epoch: m.initial_epoch,
		control: d32("manifest_prime_pin"),
		length: 137,
	};
	let mut e = TrustedEntry2::fixture(g, c, l(index), &cmd).unwrap();
	if changed {
		e.change_fingerprint();
	}
	let (_, _, _, fp, d) = e.fields();
	let verified = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
	let ce = CommittedEntry2::fixture(at(1, 1, index), fp, EntryClass2::Bootstrap(d));
	bootstrap2(&s, &ce, &e, &verified, &cmd).unwrap().0
}
fn rooted(genesis: Digest2, changed: bool) -> MutationAuthorityState2 {
	let (g, c) = gc();
	rooted_as(g, c, genesis, 2, changed)
}
fn tuple_for(manifest_bytes: &[u8], record: BootstrapRecord2) -> PinnedBoundTuple2 {
	let manifest = AdminManifest2::decode(manifest_bytes).unwrap();
	let pin = crate::storage::mutation_visibility::root2::admin::manifest::admin_manifest_digest2(
		manifest_bytes.try_into().unwrap(),
	);
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			identity: RootIdentity2 {
				generation: manifest.generation,
				cluster: manifest.cluster,
				pin,
				admin_key: manifest.admin_key,
			},
			manifest,
			state: RootState2::Bound,
			last_seq: record.inventory_seq,
			record: Some(record),
		}],
		last_seq: record.inventory_seq,
	})
	.unwrap();
	let inventory_pin = crate::storage::mutation_visibility::root2::codec::hash(b"", &snapshot);
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(manifest_bytes),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.unwrap()
}
fn record() -> BootstrapRecord2 {
	BootstrapRecord2::decode(&hex("record")).unwrap()
}
fn tuple() -> PinnedBoundTuple2 {
	tuple_for(&hex("manifest_prime"), record())
}
fn authority() -> AuthorityRef2 {
	AuthorityRef2 {
		id: [0x41; 16],
		revision: 1,
	}
}
fn enroll() -> Command2 {
	Command2::EnrollAuthority {
		authority: authority(),
		node: NodeRef2 {
			node: 7,
			incarnation: [0x17; 16],
		},
		key: [0x2B; 32],
	}
}
fn entry(c: &Command2, i: u64) -> (TrustedEntry2, VerifiedCommand2) {
	let (g, cl) = gc();
	(
		TrustedEntry2::fixture(g, cl, l(i), c).unwrap(),
		VerifiedCommand2::fixture(g, cl, c, &[], 0).unwrap(),
	)
}
fn plain(s: &MutationAuthorityState2, c: &Command2, i: u64) -> MutationAuthorityState2 {
	let (e, v) = entry(c, i);
	advance_root2(s, &e, &v, c).unwrap().0
}
fn admit(
	s: &MutationAuthorityState2,
	c: &Command2,
	i: u64,
) -> Result2<(MutationAuthorityState2, ApplyResult2)> {
	let (e, v) = entry(c, i);
	advance_admission2(s, &tuple(), &e, &v, c)
}
fn root_of(s: &MutationAuthorityState2) -> Root2 {
	root_and_origin2(s).unwrap().0.clone()
}
fn bytes_of(s: &MutationAuthorityState2) -> Vec<u8> {
	encode_authority2(s).unwrap()
}
// After every step: rebuild equality, full validation, profile-3 and authority round trips.
fn sound(s: &MutationAuthorityState2) {
	let r = root_of(s);
	assert!(r.state.derived == Derived2::build(&r.state).unwrap());
	validate_state(&r.state).unwrap();
	let b = r.encode().unwrap();
	assert_eq!(UnverifiedRoot2::decode(&b).unwrap().encode().unwrap(), b);
	let a = bytes_of(s);
	assert_eq!(
		UnverifiedAuthority2::decode(&a).unwrap().encode().unwrap(),
		a
	);
}
fn permission(outcome: u8, enrolled: u64, revoked: Option<u64>) -> ApplyResult2 {
	ApplyResult2::Permission {
		outcome,
		key: unhex(gv()["key"].as_str().unwrap()).try_into().unwrap(),
		enrolled: l(enrolled),
		revoked: revoked.map(l),
	}
}
fn is_denied(r: &ApplyResult2, reason: u16, reference: Option<Log2>) -> bool {
	matches!(r, ApplyResult2::Denied { reason: x, reference: y } if *x == reason && *y == reference)
}
fn standard() -> MutationAuthorityState2 {
	rooted(d32("genesis"), false)
}
fn enrolled() -> MutationAuthorityState2 {
	plain(&standard(), &enroll(), 3)
}
fn covered(s: &MutationAuthorityState2) -> Log2 {
	root_of(s).state.covered
}
// A coherently installed administrator of a sibling root that differs only in `genesis` or in its
// original Bootstrap fingerprint.
fn transplanted() -> Vec<InstalledAdmin2> {
	let mut other_genesis = record();
	other_genesis.identity.genesis_fingerprint = [0x77; 32];
	let a = install_admin2(
		&rooted([0x77; 32], false),
		&tuple_for(&hex("manifest_prime"), other_genesis),
	)
	.unwrap();
	let sibling = rooted(d32("genesis"), true);
	let (_, origin) = root_and_origin2(&sibling).unwrap();
	assert_ne!(
		origin.bootstrap_fingerprint,
		d32("bootstrap_fingerprint_stand_in")
	);
	let mut other_fp = record();
	other_fp.identity.bootstrap_fingerprint = origin.bootstrap_fingerprint;
	let b = install_admin2(&sibling, &tuple_for(&hex("manifest_prime"), other_fp)).unwrap();
	vec![a, b]
}

#[test]
fn gate_requires_whole_installed_administrator_context() {
	let s = enrolled();
	let before = bytes_of(&s);
	let grant = statement("grant");
	let (e, v) = entry(&grant, 4);
	// No rooted state, the wrong command, or no installable administrator: nothing consumed.
	assert_eq!(
		advance_admission2(&empty_authority2(), &tuple(), &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	let pre = advance_pre_root2(
		&empty_authority2(),
		&CommittedEntry2::fixture(at(0, 0, 0), d32("genesis"), EntryClass2::Membership),
	)
	.unwrap()
	.0;
	assert_eq!(
		advance_admission2(&pre, &tuple(), &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	let cover = Command2::Cover {
		kind: 0,
		external: [1; 32],
	};
	let (ce, cv) = entry(&cover, 4);
	assert_eq!(
		advance_admission2(&s, &tuple(), &ce, &cv, &cover).err(),
		Some(RootError2::TrustMismatch)
	);
	let original: serde_json::Value =
		serde_json::from_str(include_str!("../provisioning/tests/anchor_vectors.json")).unwrap();
	let m1 = unhex(original["manifests"]["m1"]["bytes"].as_str().unwrap());
	let mut m1_record = record();
	m1_record.identity.pin = unhex(original["manifests"]["m1"]["pin"].as_str().unwrap())
		.try_into()
		.unwrap();
	assert_eq!(
		advance_admission2(&s, &tuple_for(&m1, m1_record), &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch),
		"install refused: another manifest"
	);
	// Transplanted administrators are refused at the next index and at an exact-covered replay.
	let (granted, r) = admit(&s, &grant, 4).unwrap();
	assert!(r == permission(1, 4, None));
	for admin in transplanted() {
		assert_eq!(
			Root2::reduce_admission(&s, &admin, &e, &v, &grant).err(),
			Some(RootError2::TrustMismatch)
		);
		assert_eq!(
			Root2::reduce_admission(&granted, &admin, &e, &v, &grant).err(),
			Some(RootError2::TrustMismatch),
			"exact replay"
		);
	}
	assert_eq!(bytes_of(&s), before);
	assert_eq!(covered(&s), l(3));
}

#[test]
fn gate_order_replay_gap_and_authorization() {
	let s = enrolled();
	let before = bytes_of(&s);
	let admin = install_admin2(&s, &tuple()).unwrap();
	let grant = statement("grant");
	let (e, v) = entry(&grant, 4);
	// 0. Only this command, even when called directly.
	let cover = Command2::Cover {
		kind: 0,
		external: [2; 32],
	};
	let (ce, cv) = entry(&cover, 4);
	assert_eq!(
		Root2::reduce_admission(&s, &admin, &ce, &cv, &cover).err(),
		Some(RootError2::TrustMismatch)
	);
	// 1. Only a rooted state.
	assert_eq!(
		Root2::reduce_admission(&empty_authority2(), &admin, &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	// 2. A valid log.
	let (g, c) = gc();
	let zero = TrustedEntry2::fixture(
		g,
		c,
		Log2 {
			term: 1,
			node: 1,
			index: 0,
		},
		&grant,
	)
	.unwrap();
	assert_eq!(
		Root2::reduce_admission(&s, &admin, &zero, &v, &grant).err(),
		Some(RootError2::Codec)
	);
	// 3. Digest and verified match.
	assert_eq!(
		Root2::reduce_admission(&s, &admin, &ce, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		Root2::reduce_admission(&s, &admin, &e, &cv, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	// 4. The root the entry names (another generation; the command's own relations agree).
	let other = rooted_as([0x55; 16], c, d32("genesis"), 2, false);
	assert_eq!(
		Root2::reduce_admission(&other, &admin, &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	// 7. Gap.
	assert_eq!(admit(&s, &grant, 6).err(), Some(RootError2::LineageGap));
	// 8. Unauthorized: a consumed denial.
	let unauthorized = entry(&grant, 4).1.unauthorized();
	let (d, r) = advance_admission2(&s, &tuple(), &e, &unauthorized, &grant).unwrap();
	assert!(is_denied(&r, 8, None));
	assert_eq!(covered(&d), l(4));
	assert_eq!(root_of(&d).state.permissions.len(), 0);
	sound(&d);
	// 6. Replay after a real grant: AlreadyProcessed only after binding; a mismatched tip refuses.
	let (granted, _) = admit(&s, &grant, 4).unwrap();
	assert!(matches!(
		admit(&granted, &grant, 4).unwrap().1,
		ApplyResult2::AlreadyProcessed
	));
	let mut changed = TrustedEntry2::fixture(g, c, l(4), &grant).unwrap();
	changed.change_fingerprint();
	assert_eq!(
		advance_admission2(&granted, &tuple(), &changed, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	// Replay of a consumed invalid-signature denial is metadata idempotence, never a permission.
	let other_signer = statement("other_signer");
	let (denied, r) = admit(&s, &other_signer, 4).unwrap();
	assert!(is_denied(&r, 8, None));
	let (again, r) = admit(&denied, &other_signer, 4).unwrap();
	assert!(matches!(r, ApplyResult2::AlreadyProcessed));
	assert_eq!(root_of(&again).state.permissions.len(), 0);
	assert_eq!(bytes_of(&s), before);
}

#[test]
fn statement_failures_are_consumed_denials() {
	let s = enrolled();
	let before = bytes_of(&s);
	let mut cases = Vec::new();
	for (name, value) in gv()["context_variants"].as_object().unwrap() {
		cases.push((name.clone(), variant(value.as_str().unwrap())));
	}
	cases.push((
		"scope".into(),
		variant(gv()["scope_variant"].as_str().unwrap()),
	));
	cases.push(("other_signer".into(), statement("other_signer")));
	let mut flipped = unhex(sv()["grant"]["bytes"].as_str().unwrap());
	flipped[336] ^= 1;
	cases.push((
		"flipped".into(),
		Command2::AdmissionPermission(bytes337(flipped)),
	));
	assert_eq!(cases.len(), 9);
	for (name, c) in cases {
		let (next, r) = admit(&s, &c, 4).unwrap_or_else(|e| panic!("{name}: {e:?}"));
		assert!(is_denied(&r, 8, None), "{name}");
		assert_eq!(covered(&next), l(4), "{name}");
		assert_eq!(root_of(&next).state.permissions.len(), 0, "{name}");
		sound(&next);
	}
	// Structural and relation faults are not consumed: Codec at the digest.
	let mut magic = unhex(sv()["grant"]["bytes"].as_str().unwrap());
	magic[0] = b'X';
	let mut namespace = unhex(sv()["grant"]["bytes"].as_str().unwrap());
	namespace[224..240].fill(0xB2);
	for bad in [magic, namespace] {
		let c = Command2::AdmissionPermission(bytes337(bad));
		let (g, cl) = gc();
		assert_eq!(
			TrustedEntry2::fixture(g, cl, l(4), &c).err(),
			Some(RootError2::Codec)
		);
		assert_eq!(c.digest(g, cl).err(), Some(RootError2::Codec));
	}
	assert_eq!(bytes_of(&s), before);
}

#[test]
fn grant_and_revoke_apply_through_the_gate() {
	let (g, c) = gc();
	let grant = statement("grant");
	let revoke = statement("revoke");
	let s = enrolled();
	let (s4, r) = admit(&s, &grant, 4).unwrap();
	assert!(r == permission(1, 4, None));
	assert_eq!(
		hex_of(&r.encode(g, c, l(4)).unwrap()),
		gv()["granted"].as_str().unwrap()
	);
	// The post-grant root, independently constructed.
	assert_eq!(
		hex_of(&root_of(&s4).encode().unwrap()),
		gv()["post_grant_root"]["hex"].as_str().unwrap()
	);
	sound(&s4);
	let (s5, r) = admit(&s4, &revoke, 5).unwrap();
	assert!(r == permission(3, 4, Some(5)));
	assert_eq!(
		hex_of(&r.encode(g, c, l(5)).unwrap()),
		gv()["revoked"].as_str().unwrap()
	);
	sound(&s5);
	let (s6, r) = admit(&s5, &grant, 6).unwrap();
	assert!(r == permission(2, 4, Some(5)));
	sound(&s6);
	let (s7, r) = admit(&s6, &revoke, 7).unwrap();
	assert!(r == permission(4, 4, Some(5)));
	sound(&s7);
	// Unknown authority and unknown permission.
	let (x, r) = admit(&standard(), &grant, 3).unwrap();
	assert!(is_denied(&r, 9, None));
	sound(&x);
	let (x, r) = admit(&standard(), &revoke, 3).unwrap();
	assert!(is_denied(&r, 9, None));
	sound(&x);
	// Revoked authority.
	let retired = plain(&s, &Command2::RevokeAuthority(authority()), 4);
	let (x, r) = admit(&retired, &grant, 5).unwrap();
	assert!(is_denied(&r, 16, Some(l(4))));
	sound(&x);
	// Capacity under a caller-lowered permission limit.
	let tight = lower_limits_for_test(
		&s,
		Limits2 {
			permissions: 0,
			..Limits2::default()
		},
	);
	let (x, r) = admit(&tight, &grant, 4).unwrap();
	assert!(is_denied(&r, 11, None));
	assert_eq!(root_of(&x).state.permissions.len(), 0);
}

#[test]
fn timelines_t1_to_t6_end_to_end() {
	let grant = statement("grant");
	let revoke = statement("revoke");
	// T1-T3 with the same statement bytes in later entries.
	let (s, r) = admit(&enrolled(), &grant, 4).unwrap();
	assert!(r == permission(1, 4, None));
	let (s, r) = admit(&s, &grant, 5).unwrap();
	assert!(r == permission(2, 4, None), "T1");
	let (s, r) = admit(&s, &revoke, 6).unwrap();
	assert!(r == permission(3, 4, Some(6)));
	let (s, r) = admit(&s, &grant, 7).unwrap();
	assert!(r == permission(2, 4, Some(6)), "T2: no revival");
	let (s, r) = admit(&s, &revoke, 8).unwrap();
	assert!(r == permission(4, 4, Some(6)), "T3");
	sound(&s);
	// T4: a revoke before the grant is denied, then the same bytes revoke after the grant.
	let (s, r) = admit(&enrolled(), &revoke, 4).unwrap();
	assert!(is_denied(&r, 9, None));
	let (s, r) = admit(&s, &grant, 5).unwrap();
	assert!(r == permission(1, 5, None));
	let (s, r) = admit(&s, &revoke, 6).unwrap();
	assert!(r == permission(3, 5, Some(6)), "T4");
	sound(&s);
	// T5: refused for an unknown authority, effective after enrollment.
	let (s, r) = admit(&standard(), &grant, 3).unwrap();
	assert!(is_denied(&r, 9, None));
	let s = plain(&s, &enroll(), 4);
	let (s, r) = admit(&s, &grant, 5).unwrap();
	assert!(r == permission(1, 5, None), "T5");
	sound(&s);
	// T6: a revoked authority revision is never eligible again.
	let s = plain(&enrolled(), &Command2::RevokeAuthority(authority()), 4);
	let (s, r) = admit(&s, &grant, 5).unwrap();
	assert!(is_denied(&r, 16, Some(l(4))));
	let (s, r) = admit(&s, &grant, 6).unwrap();
	assert!(is_denied(&r, 16, Some(l(4))), "T6");
	sound(&s);
}

#[test]
fn legacy_paths_still_refuse_tag_eleven() {
	let s = enrolled();
	let grant = statement("grant");
	let (e, v) = entry(&grant, 4);
	assert_eq!(
		advance_root2(&s, &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		Root2::reduce(Some(&root_of(&s)), &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
}
fn hex_of(b: &[u8]) -> String {
	b.iter().map(|x| format!("{x:02x}")).collect()
}
