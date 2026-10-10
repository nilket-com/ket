//! Dormant Admission permission command (tag 11) and result (tag 9). Structural inspection is never
//! authentication, and `Root2::reduce` refuses the command before replay, gap or authorization.
use super::*;
use crate::storage::mutation_visibility::root2::admin_statement::*;
use crate::storage::mutation_visibility::root2::admission_registry::*;
use crate::storage::mutation_visibility::root2::durable::{
	JournalIdentity2, UnverifiedGrantArchive2, UnverifiedJournal2, UnverifiedJournalStep2,
	UnverifiedOutcome2,
};
use crate::storage::mutation_visibility::root2::incremental::{Derived2, update};

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn v() -> serde_json::Value {
	serde_json::from_str(include_str!("admission_command_vectors.json")).unwrap()
}
fn hex16(field: &str) -> [u8; 16] {
	unhex(v()[field].as_str().unwrap()).try_into().unwrap()
}
fn statement(name: &str) -> [u8; 337] {
	let s: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	unhex(s[name]["bytes"].as_str().unwrap())
		.try_into()
		.unwrap()
}
fn cmd(name: &str) -> Command2 {
	Command2::AdmissionPermission(statement(name))
}
fn processed() -> Log2 {
	l(10)
}
fn gc() -> ([u8; 16], [u8; 16]) {
	(hex16("generation"), hex16("cluster"))
}
fn bootstrapped() -> Root2 {
	let (g, c) = gc();
	let boot = Command2::Bootstrap {
		epoch: 1,
		control: [9; 32],
		length: 137,
	};
	let e = TrustedEntry2::fixture(g, c, l(1), &boot).unwrap();
	let v = VerifiedCommand2::fixture(g, c, &boot, &[], 0).unwrap();
	Root2::reduce(None, &e, &v, &boot).unwrap().0
}
fn reduce_at(r: Option<&Root2>, c: &Command2, at: Log2) -> Result2<(Root2, ApplyResult2)> {
	let (g, cl) = gc();
	let e = TrustedEntry2::fixture(g, cl, at, c).unwrap();
	let v = VerifiedCommand2::fixture(g, cl, c, &[], 0).unwrap();
	Root2::reduce(r, &e, &v, c)
}
// A test-built root whose covered tip is exactly the entry for `c` at `at`.
fn tip_at(r: &Root2, c: &Command2, at: Log2) -> Root2 {
	let (g, cl) = gc();
	let e = TrustedEntry2::fixture(g, cl, at, c).unwrap();
	let (_, _, log, fp, d) = e.fields();
	let mut t = r.clone();
	t.state.covered = log;
	t.state.fingerprint = fp;
	t.state.command_digest = d;
	t
}

#[test]
fn inspection_is_structural_and_not_authorizing() {
	let (g, c) = gc();
	let grant = statement("grant");
	let f = inspect_admin_statement2(&grant).unwrap();
	assert_eq!(f.action, AdminAction2::Grant);
	assert_eq!(f.key, decode_permission_key2(&grant[200..273]).unwrap());
	assert_eq!((f.generation, f.cluster), (g, c));
	assert_eq!(
		inspect_admin_statement2(&statement("revoke"))
			.unwrap()
			.action,
		AdminAction2::Revoke
	);
	// A statement from another signer, or with a flipped signature bit, inspects identically:
	// inspection says nothing about authentication (check_admin_statement2 refuses other_signer,
	// pinned in the statement tests).
	assert_eq!(
		inspect_admin_statement2(&statement("other_signer")).unwrap(),
		f
	);
	let mut flipped = grant;
	flipped[336] ^= 1;
	assert_eq!(inspect_admin_statement2(&flipped).unwrap(), f);
	// Structural faults: same class as the accepted structural parser.
	for (at, value) in [
		(0usize, b'X'),
		(8, 9),
		(10, 3),
		(11, 1),
		(12, 0),
		(200, 0),
		(248, 7),
	] {
		let mut b = grant;
		if at == 12 || at == 200 {
			b[at..at + 16].fill(value);
		} else {
			b[at] = value;
		}
		let ours = inspect_admin_statement2(&b).map(|_| ()).err();
		let theirs = admin_statement_preimage2(&b).map(|_| ()).err();
		assert!(ours.is_some());
		assert_eq!(ours, theirs, "offset {at}");
	}
	assert_eq!(
		inspect_admin_statement2(&grant[..336]).err(),
		Some(StatementError2::Length)
	);
}

#[test]
fn admission_command_codec_digest_and_goldens() {
	let (g, c) = gc();
	for name in ["grant", "revoke", "other_signer"] {
		let bytes = unhex(v()["commands"][name]["bytes"].as_str().unwrap());
		let (dg, dc, decoded) = Command2::decode(&bytes).unwrap();
		assert_eq!((dg, dc), (g, c));
		assert!(decoded == cmd(name), "{name}");
		assert_eq!(decoded.encode(g, c).unwrap(), bytes, "{name}");
		assert_eq!(
			hex(&decoded.digest(g, c).unwrap()),
			v()["commands"][name]["digest"].as_str().unwrap()
		);
	}
	for (name, value) in v()["command_refusals"].as_object().unwrap() {
		let bytes = unhex(value.as_str().unwrap());
		assert_eq!(
			Command2::decode(&bytes).err(),
			Some(RootError2::Codec),
			"{name}"
		);
	}
	// The same relations on the encode side: payload, encode and digest refuse them.
	let grant = cmd("grant");
	for (gg, cc) in [([0x99; 16], c), (g, [0x98; 16])] {
		assert_eq!(grant.payload(gg, cc).err(), Some(RootError2::Codec));
		assert_eq!(grant.encode(gg, cc).err(), Some(RootError2::Codec));
		assert_eq!(grant.digest(gg, cc).err(), Some(RootError2::Codec));
	}
	let mut ns = statement("grant");
	ns[224..240].fill(0xB2);
	assert_eq!(
		Command2::AdmissionPermission(ns).encode(g, c).err(),
		Some(RootError2::Codec)
	);
	let mut bad = statement("grant");
	bad[0] = b'X';
	assert_eq!(
		Command2::AdmissionPermission(bad).encode(g, c).err(),
		Some(RootError2::Codec)
	);
	// Durable grant archive, two separate real paths (neither is a Grant-only kind check on bytes):
	// a complete archive is over the outer cap, and the in-memory object fails its Grant pattern.
	let command = grant.encode(g, c).unwrap();
	assert_eq!(command.len(), 418);
	let mut body = Vec::new();
	body.extend(g);
	body.extend(c);
	body.extend(l(1).term.to_le_bytes());
	body.extend(l(1).node.to_le_bytes());
	body.extend(l(1).index.to_le_bytes());
	body.extend(l(2).term.to_le_bytes());
	body.extend(l(2).node.to_le_bytes());
	body.extend(l(2).index.to_le_bytes());
	body.extend([3; 32]);
	body.extend((command.len() as u32).to_le_bytes());
	body.extend(&command);
	body.extend([4; 32]);
	body.extend([5; 32]);
	let archive = envelope(b"VISGRP02", &body, 1 << 20).unwrap();
	assert_eq!(archive.len(), 638);
	assert_eq!(
		UnverifiedGrantArchive2::decode(&archive).err(),
		Some(RootError2::Limit)
	);
	let object = UnverifiedGrantArchive2 {
		generation: g,
		cluster: c,
		bootstrap: l(1),
		applied: l(2),
		entry_context: [3; 32],
		command: grant,
		original_digest: [4; 32],
		reference_context: [5; 32],
	};
	assert_eq!(object.encode().err(), Some(RootError2::Codec));
}

#[test]
fn permission_result_codec_and_goldens() {
	let (g, c) = gc();
	let p = processed();
	for (name, value) in v()["results"].as_object().unwrap() {
		let bytes = unhex(value.as_str().unwrap());
		let (dg, dc, dl, r) = ApplyResult2::decode(&bytes).unwrap();
		assert_eq!((dg, dc, dl), (g, c, p), "{name}");
		assert!(matches!(r, ApplyResult2::Permission { .. }));
		assert_eq!(r.encode(g, c, p).unwrap(), bytes, "{name}");
	}
	for (name, value) in v()["result_refusals"].as_object().unwrap() {
		let bytes = unhex(value.as_str().unwrap());
		assert_eq!(
			ApplyResult2::decode(&bytes).err(),
			Some(RootError2::Codec),
			"{name}"
		);
	}
	// Encode-side refusals, including same index with a changed term or node.
	let key: [u8; 73] = unhex(v()["key"].as_str().unwrap()).try_into().unwrap();
	let at = |term, node, index| Log2 { term, node, index };
	let result = |outcome, enrolled, revoked| ApplyResult2::Permission {
		outcome,
		key,
		enrolled,
		revoked,
	};
	for bad in [
		result(1, at(2, 1, 10), None),
		result(1, at(1, 2, 10), None),
		result(3, at(1, 1, 3), Some(at(2, 1, 10))),
		result(3, at(1, 1, 3), Some(at(1, 2, 10))),
		result(2, at(1, 1, 10), None),
		result(4, at(1, 1, 3), None),
		result(4, at(1, 1, 3), Some(at(1, 1, 10))),
		result(0, at(1, 1, 3), None),
		result(5, at(1, 1, 3), None),
		result(2, at(1, 1, 5), Some(at(1, 1, 5))),
	] {
		assert_eq!(bad.encode(g, c, p).err(), Some(RootError2::Codec));
	}
	assert_eq!(
		result(2, at(1, 1, 3), None).encode([0x77; 16], c, p).err(),
		Some(RootError2::Codec)
	);
	assert!(result(1, p, None).encode(g, c, p).is_ok());
	// Durable consumers refuse a permission result at their kind check: every enclosing field and
	// relation before it is valid (asserted), so the refusal is not an outer-frame artifact.
	let granted = unhex(v()["results"]["granted"].as_str().unwrap());
	let (rg, rc, rl, _) = ApplyResult2::decode(&granted).unwrap();
	let mut identity = claim().manifest.identity;
	identity.generation = g;
	assert_eq!((rg, rc, rl), (g, c, p));
	let journal = UnverifiedJournal2 {
		owner: JournalIdentity2 {
			generation: g,
			cluster: c,
			identity,
			claim: [6; 32],
			reservation: l(3),
		},
		sequence: 2,
		previous: [7; 32],
		step: UnverifiedJournalStep2::TerminalObserved {
			result: granted.clone(),
			processed: p,
		},
	};
	assert_eq!(journal.encode().err(), Some(RootError2::Codec));
	let mut body = Vec::new();
	body.extend(g);
	body.extend(c);
	body.extend(2u64.to_le_bytes());
	body.extend([7; 32]);
	body.extend(identity.encode().unwrap());
	body.extend([6; 32]);
	for x in [
		l(3).term.to_le_bytes().to_vec(),
		l(3).node.to_le_bytes().to_vec(),
		l(3).index.to_le_bytes().to_vec(),
	] {
		body.extend(x);
	}
	body.push(4);
	let mut step = Vec::new();
	step.extend((granted.len() as u32).to_le_bytes());
	step.extend(&granted);
	step.extend(p.term.to_le_bytes());
	step.extend(p.node.to_le_bytes());
	step.extend(p.index.to_le_bytes());
	body.extend((step.len() as u32).to_le_bytes());
	body.extend(step);
	let bytes = envelope(b"VISJRN02", &body, 1 << 21).unwrap();
	assert_eq!(
		UnverifiedJournal2::decode(&bytes).err(),
		Some(RootError2::Codec)
	);
	let outcome = UnverifiedOutcome2 {
		generation: g,
		cluster: c,
		identity,
		claim: [6; 32],
		decision: l(9),
		result: granted,
		response: Vec::new(),
	};
	assert!(outcome.decision.index <= p.index && outcome.identity.generation == g);
	assert_eq!(outcome.encode().err(), Some(RootError2::Codec));
	// The local store scan only sees journal records that passed the decode above, and keeps its
	// own Parent-only refusal.
	let scan = include_str!("../durable/local_store/scan.rs");
	assert_eq!(
		scan.matches("ApplyResult2::Parent { record, .. }) = ApplyResult2::decode(")
			.count(),
		2
	);
}

#[test]
fn admission_dispatch_fails_closed_before_replay() {
	let r = bootstrapped();
	let before = r.encode().unwrap();
	let unchanged = |r: &Root2| {
		assert_eq!(r.encode().unwrap(), before);
		assert_eq!(r.state.permissions.len(), 0);
		assert!(r.state.derived == Derived2::build(&r.state).unwrap());
	};
	let grant = cmd("grant");
	// Next index.
	assert_eq!(
		reduce_at(Some(&r), &grant, l(2)).err(),
		Some(RootError2::TrustMismatch)
	);
	unchanged(&r);
	// Exact covered index whose tip matches this very entry: still no AlreadyProcessed.
	let tip = tip_at(&r, &grant, l(2));
	assert_eq!(
		reduce_at(Some(&tip), &grant, l(2)).err(),
		Some(RootError2::TrustMismatch)
	);
	let cover = Command2::Cover {
		kind: 0,
		external: [1; 32],
	};
	let control = tip_at(&r, &cover, l(2));
	assert!(matches!(
		reduce_at(Some(&control), &cover, l(2)).unwrap().1,
		ApplyResult2::AlreadyProcessed
	));
	// Gap: refused before the gap check (the Cover control reaches LineageGap).
	assert_eq!(
		reduce_at(Some(&r), &grant, l(5)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		reduce_at(Some(&r), &cover, l(5)).err(),
		Some(RootError2::LineageGap)
	);
	// Unauthorized fixture: not Denied(8).
	let (g, c) = gc();
	let e = TrustedEntry2::fixture(g, c, l(2), &grant).unwrap();
	let v = VerifiedCommand2::fixture(g, c, &grant, &[], 0)
		.unwrap()
		.unauthorized();
	assert_eq!(
		Root2::reduce(Some(&r), &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	// Invalid signatures: still structurally valid, still refused at the dormant gate.
	let mut flipped = statement("grant");
	flipped[336] ^= 1;
	for bad in [cmd("other_signer"), Command2::AdmissionPermission(flipped)] {
		assert_eq!(
			reduce_at(Some(&r), &bad, l(2)).err(),
			Some(RootError2::TrustMismatch)
		);
	}
	// Earlier stages keep their errors: digest mismatch, a root of another generation (the
	// command's own relations agree with the entry), and no root at all.
	let ce = TrustedEntry2::fixture(g, c, l(2), &cover).unwrap();
	let cv = VerifiedCommand2::fixture(g, c, &grant, &[], 0).unwrap();
	assert_eq!(
		Root2::reduce(Some(&r), &ce, &cv, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	let other = root("root_bootstrap_only");
	assert_ne!(other.state.generation, g);
	assert_eq!(
		reduce_at(Some(&other), &grant, l(2)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		reduce_at(None, &grant, l(1)).err(),
		Some(RootError2::TrustMismatch)
	);
	// A malformed relation never reaches the reducer: the entry digest itself is refused.
	assert_eq!(grant.digest([0x99; 16], c).err(), Some(RootError2::Codec));
	unchanged(&r);
}

#[test]
fn permission_update_arm_tracks_grant_and_revoke() {
	// The new root golden: authority 0x41 rev 1 enrolled at 2, permissions at 3/4/5. The grant
	// statement names the same authority and generation with a distinct Table key.
	let p: serde_json::Value =
		serde_json::from_str(include_str!("root_profile_vectors.json")).unwrap();
	let b = unhex(p["root_with_permissions"]["hex"].as_str().unwrap());
	let u = UnverifiedRoot2::decode(&b).unwrap();
	let (g, c, l0, fp) = u.fixture_fields();
	let base = u
		.fixture_install(g, c, l0, fp, hash(b"", &b))
		.unwrap()
		.state;
	let grant = cmd("grant");
	let key = inspect_admin_statement2(&statement("grant")).unwrap().key;
	assert_eq!(key.namespace.generation, base.generation);
	let authority = base.authorities.values().next().unwrap().clone();
	assert_eq!(authority.authority, key.authority);
	let step = |old: &State2, m: PermissionMap2, at: u64, c: &Command2| {
		let mut next = old.clone();
		next.permissions = m;
		next.covered = l(at);
		validate_state(&next).unwrap();
		update(old, &mut next, c).unwrap();
		assert!(next.derived == Derived2::build(&next).unwrap(), "at {at}");
		next
	};
	let mut old = base.clone();
	old.covered = l(6);
	validate_state(&old).unwrap();
	let used = |s: &State2| usage(s).unwrap().registry;
	// Granted: +117 and one coordinate.
	let (m, o, _) = grant_permission2(
		&old.permissions,
		Some(&authority),
		&key,
		l(7),
		used(&old),
		permission_limits2(),
	)
	.unwrap();
	assert!(matches!(o, PermissionOutcome2::Granted(_)));
	let granted = step(&old, m, 7, &grant);
	assert_eq!(
		granted.derived.usage.registry,
		old.derived.usage.registry + 117
	);
	assert_eq!(
		granted.derived.non_grants.len(),
		old.derived.non_grants.len() + 1
	);
	// GrantDuplicate: no bytes, no coordinate.
	let (m, o, _) = grant_permission2(
		&granted.permissions,
		Some(&authority),
		&key,
		l(8),
		used(&granted),
		permission_limits2(),
	)
	.unwrap();
	assert!(matches!(o, PermissionOutcome2::GrantDuplicate(_)));
	let dup = step(&granted, m, 8, &grant);
	assert!(dup.derived == granted.derived);
	// Revoked: one coordinate, no bytes.
	let revoke = cmd("revoke");
	let (m, o, _) = revoke_permission2(&dup.permissions, &key, l(9)).unwrap();
	assert!(matches!(o, PermissionOutcome2::Revoked(_)));
	let revoked = step(&dup, m, 9, &revoke);
	assert_eq!(revoked.derived.usage.registry, dup.derived.usage.registry);
	assert_eq!(
		revoked.derived.non_grants.len(),
		dup.derived.non_grants.len() + 1
	);
	// RevokeDuplicate and an unknown-authority denial: unchanged maps, no bytes, no coordinate.
	let (m, o, _) = revoke_permission2(&revoked.permissions, &key, l(10)).unwrap();
	assert!(matches!(o, PermissionOutcome2::RevokeDuplicate(_)));
	let again = step(&revoked, m, 10, &revoke);
	assert!(again.derived == revoked.derived);
	let mut stranger = key;
	stranger.authority.revision = 9;
	let (m, o, _) = grant_permission2(
		&again.permissions,
		None,
		&stranger,
		l(11),
		used(&again),
		permission_limits2(),
	)
	.unwrap();
	assert!(matches!(o, PermissionOutcome2::UnknownAuthority));
	let denied = step(&again, m, 11, &grant);
	assert!(denied.derived == again.derived);
}

#[test]
fn tag_eleven_is_outside_oracle_equivalence() {
	// The frozen oracle's one compile-only arm refuses at its own (later) stage; production refuses
	// earlier. Only the next-index case coincides, and it is the only one compared.
	let r = bootstrapped();
	let legacy = super::s1c_oracle::legacy_profile2(&r.encode().unwrap()).unwrap();
	let (g, c) = gc();
	let grant = cmd("grant");
	let e = TrustedEntry2::fixture(g, c, l(2), &grant).unwrap();
	let v = VerifiedCommand2::fixture(g, c, &grant, &[], 0).unwrap();
	assert_eq!(
		super::s1c_oracle::reduce(&legacy, Limits2::default(), &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		Root2::reduce(Some(&r), &e, &v, &grant).err(),
		Some(RootError2::TrustMismatch)
	);
	// No tag-11 command goes through the oracle-comparing step helper.
	let me = include_str!("admission_command.rs");
	assert!(!me.contains(concat!("checked", "_step(")) && !me.contains(concat!("next", "(&")));
	for other in [
		include_str!("s1c.rs"),
		include_str!("s1c_review.rs"),
		include_str!("support.rs"),
		include_str!("transitions.rs"),
		include_str!("r0_assay.rs"),
	] {
		assert!(!other.contains("AdmissionPermission"));
	}
}
fn hex(d: &[u8]) -> String {
	d.iter().map(|b| format!("{b:02x}")).collect()
}
