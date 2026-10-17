//! Stage C narrow slice: command verification through `advance_verified2` only. Everything C
//! cannot produce (enrollment, grants, revocation, coverage, raw comparisons) is built with
//! clearly labelled FIXTURE transitions that use `VerifiedCommand2::fixture`. Governed Bootstrap
//! and AdmissionPermission use the G1d gate fixtures; Reserve and Finalize use the auth KAT claim
//! and holder attestation (RFC 8032 TEST 1 public seed) on an ungoverned root.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifest2, admin_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_statement::admin_statement_preimage2;
use crate::storage::mutation_visibility::root2::auth::preimage::{
	admission_preimage2, holder_preimage2,
};
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use crate::storage::mutation_visibility::root2::verification::*;
use ed25519_dalek::Signer;

type Out = Result<(MutationAuthorityState2, ApplyResult2), VerifyError2>;

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn sv(field: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	unhex(v[field].as_str().unwrap())
}
fn kat(field: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../auth/tests/vectors.json")).unwrap();
	unhex(v[field].as_str().unwrap())
}
fn vv(field: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("verification_vectors.json")).unwrap();
	unhex(v[field].as_str().unwrap())
}
fn d32(v: Vec<u8>) -> Digest2 {
	v.try_into().unwrap()
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}

// ---- Governed fixtures (G1d gate) ----
fn manifest_prime() -> AdminManifest2 {
	AdminManifest2::decode(&sv("manifest_prime")).unwrap()
}
fn record() -> BootstrapRecord2 {
	BootstrapRecord2::decode(&sv("record")).unwrap()
}
fn tuple_for(record: BootstrapRecord2) -> PinnedBoundTuple2 {
	let bytes = sv("manifest_prime");
	let manifest = AdminManifest2::decode(&bytes).unwrap();
	let pin = admin_manifest_digest2(bytes.as_slice().try_into().unwrap());
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			terminal: None,
			identity: RootIdentity2 {
				generation: manifest.generation,
				cluster: manifest.cluster,
				pin,
				admin_key: manifest.admin_key,
			},
			manifest: AnyAdminManifest2::One(manifest),
			state: RootState2::Bound,
			last_seq: record.inventory_seq,
			record: Some(record),
		}],
		last_seq: record.inventory_seq,
	})
	.unwrap();
	let inventory_pin = crate::storage::mutation_visibility::root2::codec::hash(b"", &snapshot);
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&bytes),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.unwrap()
}
fn tuple() -> PinnedBoundTuple2 {
	tuple_for(record())
}
/// FIXTURE: genesis, one scanned entry; a pre-root state ready for a Bootstrap at (1,1,2).
fn pre_root(genesis: Digest2) -> MutationAuthorityState2 {
	let first = CommittedEntry2::fixture(at(0, 0, 0), genesis, EntryClass2::Membership);
	let s = advance_pre_root2(&empty_authority2(), &first).unwrap().0;
	let one = CommittedEntry2::fixture(at(1, 1, 1), [1; 32], EntryClass2::NonRoot2);
	advance_pre_root2(&s, &one).unwrap().0
}
fn governed(epoch_delta: u64, control: Option<Digest2>, length: u64) -> Command2 {
	Command2::Bootstrap {
		epoch: manifest_prime().initial_epoch + epoch_delta,
		control: control.unwrap_or_else(|| d32(sv("manifest_prime_pin"))),
		length,
	}
}
fn bootstrap_inputs(g: [u8; 16], c: [u8; 16], cmd: &Command2) -> (CommittedEntry2, TrustedEntry2) {
	let e = TrustedEntry2::fixture(g, c, l(2), cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	(
		CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap(d)),
		e,
	)
}
/// The governed root, rooted THROUGH C.
fn governed_root() -> MutationAuthorityState2 {
	let m = manifest_prime();
	let cmd = governed(0, None, 137);
	let (ce, e) = bootstrap_inputs(m.generation, m.cluster, &cmd);
	let (s, r) =
		advance_verified2(&pre_root(d32(sv("genesis"))), &tuple(), &ce, &e, &cmd, None).unwrap();
	assert!(r == ApplyResult2::Registered(l(2)));
	s
}

// ---- Shared step helpers ----
fn entry_of(g: [u8; 16], c: [u8; 16], cmd: &Command2, i: u64) -> (CommittedEntry2, TrustedEntry2) {
	let e = TrustedEntry2::fixture(g, c, l(i), cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	(
		CommittedEntry2::fixture(at(1, 1, i), fp, EntryClass2::OtherRoot2(d)),
		e,
	)
}
/// Through C.
fn via_c(s: &MutationAuthorityState2, g: [u8; 16], c: [u8; 16], cmd: &Command2, i: u64) -> Out {
	let (ce, e) = entry_of(g, c, cmd, i);
	advance_verified2(s, &tuple(), &ce, &e, cmd, None)
}
/// FIXTURE: the raw consumer with a fixture token (always authorized), for comparison.
fn raw(
	s: &MutationAuthorityState2,
	g: [u8; 16],
	c: [u8; 16],
	cmd: &Command2,
	i: u64,
	evidence: u8,
) -> (MutationAuthorityState2, ApplyResult2) {
	let e = TrustedEntry2::fixture(g, c, l(i), cmd).unwrap();
	let v = VerifiedCommand2::fixture(g, c, cmd, &[], evidence).unwrap();
	match cmd {
		Command2::AdmissionPermission(_) => advance_admission2(s, &tuple(), &e, &v, cmd).unwrap(),
		_ => advance_root2(s, &e, &v, cmd).unwrap(),
	}
}
/// A result's shape, for assertion messages (ApplyResult2 has no Debug).
fn describe(r: &ApplyResult2) -> String {
	match r {
		ApplyResult2::Denied { reason, .. } => format!("Denied({reason})"),
		ApplyResult2::Parent { new_effect, .. } => format!("Parent(new_effect={new_effect})"),
		ApplyResult2::Registered(l) => format!("Registered({})", l.index),
		ApplyResult2::Covered => "Covered".into(),
		ApplyResult2::AlreadyProcessed => "AlreadyProcessed".into(),
		ApplyResult2::PendingEvidence(n) => format!("PendingEvidence({n})"),
		ApplyResult2::RejectedEvidence(n) => format!("RejectedEvidence({n})"),
		_ => "other".into(),
	}
}
fn parents(s: &MutationAuthorityState2) -> usize {
	root_and_origin2(s).unwrap().0.state.parents.len()
}
fn bytes(s: &MutationAuthorityState2) -> Vec<u8> {
	encode_authority2(s).unwrap()
}
fn tip(s: &MutationAuthorityState2) -> Log2 {
	root_and_origin2(s).unwrap().0.state.covered
}
/// A consumed denial: the semantic state is `before`'s, only the tip advanced to entry `i`.
fn consumed(before: &MutationAuthorityState2, out: Out, reason: u16, i: u64) {
	let (after, r) = out.unwrap();
	assert!(
		matches!(r, ApplyResult2::Denied { reason: x, .. } if x == reason),
		"expected Denied({reason})"
	);
	assert_eq!(tip(&after), l(i));
	let a = root_and_origin2(&after).unwrap().0;
	let normalized = with_tip_for_test(
		before,
		a.state.covered,
		a.state.fingerprint,
		a.state.command_digest,
	);
	assert_eq!(bytes(&normalized), bytes(&after), "no other effect");
}

// ---- KAT fixtures (ungoverned root: no Admission permission rule) ----
fn g() -> [u8; 16] {
	kat("generation").try_into().unwrap()
}
fn c() -> [u8; 16] {
	kat("cluster").try_into().unwrap()
}
fn claim() -> ClaimProjection2 {
	ClaimProjection2::decode(&kat("claim_c2")).unwrap()
}
fn holder() -> HolderAttestation2 {
	HolderAttestation2::decode(&kat("holder_signed")).unwrap()
}
fn reserve() -> Command2 {
	Command2::Reserve(claim())
}
fn finalize(h: HolderAttestation2) -> Command2 {
	let cl = claim();
	Command2::Finalize {
		identity: cl.manifest.identity,
		digest: cl.digest().unwrap(),
		reservation: l(7),
		attestation: h,
	}
}
fn revoke() -> Command2 {
	Command2::RevokeAuthority(claim().authority)
}
/// FIXTURE history through raw consumers: Bootstrap (2), EnrollSelector (3), Grant (4),
/// EnrollAuthority with `key` (5, if `enroll`), then a renewal Grant or a Blank cover at 6. Ready
/// for a Reserve at 7.
fn kat_root(cluster: [u8; 16], key: Digest2, enroll: bool, renew: bool) -> MutationAuthorityState2 {
	let cl = claim();
	let o = cl.original.clone();
	assert_eq!(o.log, l(4), "the KAT grant sits at (1,1,4)");
	let boot = Command2::Bootstrap {
		epoch: o.lease.epoch,
		control: [0x9f; 32],
		length: 0,
	};
	let (ce, e) = bootstrap_inputs(g(), cluster, &boot);
	let v = VerifiedCommand2::fixture(g(), cluster, &boot, &[], 0).unwrap();
	let mut s = bootstrap2(&pre_root([0x11; 32]), &ce, &e, &v, &boot)
		.unwrap()
		.0;
	let grant = |lease: SignedLease2| Command2::Grant {
		lineage: o.lineage,
		incarnation: o.incarnation,
		lease,
		predecessor: o.predecessor,
	};
	s = raw(
		&s,
		g(),
		cluster,
		&Command2::EnrollSelector(cl.manifest.binding()),
		3,
		0,
	)
	.0;
	s = raw(&s, g(), cluster, &grant(o.lease.clone()), 4, 0).0;
	let enrollment = Command2::EnrollAuthority {
		authority: cl.authority,
		node: NodeRef2 {
			node: o.lease.holder,
			incarnation: o.incarnation,
		},
		key,
	};
	s = if enroll {
		raw(&s, g(), cluster, &enrollment, 5, 0).0
	} else {
		raw(
			&s,
			g(),
			cluster,
			&Command2::EnrollSelector(cl.manifest.binding()),
			5,
			0,
		)
		.0
	};
	if renew {
		let mut lease = o.lease.clone();
		lease.expiry.physical_ms += 1000;
		raw(&s, g(), cluster, &grant(lease), 6, 0).0
	} else {
		let blank = CommittedEntry2::fixture(at(1, 1, 6), [6; 32], EntryClass2::Blank);
		advance_cover2(&s, &blank).unwrap().0
	}
}
fn standard() -> MutationAuthorityState2 {
	kat_root(c(), d32(kat("public_key")), true, false)
}
/// A Reserve at 7 through C, then the given number of FIXTURE revocations.
fn reserved(revoked: bool) -> MutationAuthorityState2 {
	let s = via_c(&standard(), g(), c(), &reserve(), 7).unwrap().0;
	if revoked {
		raw(&s, g(), c(), &revoke(), 8, 0).0
	} else {
		s
	}
}
fn resigned(mut h: HolderAttestation2) -> HolderAttestation2 {
	let key = ed25519_dalek::SigningKey::from_bytes(&d32(kat("public_test_signing_seed")));
	h.signature = key
		.sign(&holder_preimage2(&claim(), c(), &h).unwrap())
		.to_bytes();
	h
}

#[test]
fn bootstrap_authorized_only_by_exact_pin() {
	let m = manifest_prime();
	let (mg, mc) = (m.generation, m.cluster);
	let genesis = d32(sv("genesis"));
	assert!(origin_view2(&governed_root()) == OriginView2::Root);
	// Each case changes exactly one anchor: the entry/command side is changed and the free record
	// fields (fingerprint, command digest) are re-pointed at it, or one record field is changed.
	type Edit = fn(&mut BootstrapRecord2);
	let none: Edit = |_| {};
	let cases: Vec<(&str, [u8; 16], [u8; 16], Command2, Edit)> = vec![
		("generation", [0x5a; 16], mc, governed(0, None, 137), none),
		("cluster", mg, [0x5b; 16], governed(0, None, 137), none),
		("initial_epoch", mg, mc, governed(1, None, 137), none),
		("control", mg, mc, governed(0, Some([0x55; 32]), 137), none),
		("length", mg, mc, governed(0, None, 189), none),
		("bootstrap", mg, mc, governed(0, None, 137), |r| {
			r.identity.bootstrap = l(3)
		}),
		(
			"bootstrap_fingerprint",
			mg,
			mc,
			governed(0, None, 137),
			|r| r.identity.bootstrap_fingerprint = [0x99; 32],
		),
		("bootstrap_command", mg, mc, governed(0, None, 137), |r| {
			r.identity.bootstrap_command = [0x98; 32]
		}),
		("genesis_fingerprint", mg, mc, governed(0, None, 137), |r| {
			r.identity.genesis_fingerprint = [0x77; 32]
		}),
	];
	for (name, gg, cc, cmd, edit) in cases {
		let (ce, e) = bootstrap_inputs(gg, cc, &cmd);
		let (_, _, _, fp, d) = e.fields();
		let mut r = record();
		r.identity.bootstrap_fingerprint = fp;
		r.identity.bootstrap_command = d;
		edit(&mut r);
		let pin = tuple_for(r);
		let pre = pre_root(genesis);
		assert_eq!(
			advance_verified2(&pre, &pin, &ce, &e, &cmd, None).err(),
			Some(VerifyError2::Consumer(RootError2::TrustMismatch)),
			"{name}"
		);
		// The same inputs with an always-authorized FIXTURE token root: only C's verdict refused.
		let v = VerifiedCommand2::fixture(gg, cc, &cmd, &[], 0).unwrap();
		assert!(
			bootstrap2(&pre, &ce, &e, &v, &cmd).is_ok(),
			"{name} control"
		);
	}
	// State kind, before the consumer's committed binding.
	let cmd = governed(0, None, 137);
	let (ce, e) = bootstrap_inputs(mg, mc, &cmd);
	let wrong = CommittedEntry2::fixture(at(1, 1, 9), [0; 32], EntryClass2::Blank);
	for (s, committed) in [
		(empty_authority2(), &ce),
		(governed_root(), &ce),
		(empty_authority2(), &wrong),
	] {
		assert_eq!(
			advance_verified2(&s, &tuple(), committed, &e, &cmd, None).err(),
			Some(VerifyError2::StateKind)
		);
	}
}

#[test]
fn admission_permission_advances_a_rooted_state() {
	let m = manifest_prime();
	let (mg, mc) = (m.generation, m.cluster);
	// FIXTURE: enroll the statement's authority at 3 (EnrollAuthority is unsupported by C).
	let enroll = Command2::EnrollAuthority {
		authority: AuthorityRef2 {
			id: [0x41; 16],
			revision: 1,
		},
		node: NodeRef2 {
			node: 7,
			incarnation: [0x17; 16],
		},
		key: [0x2B; 32],
	};
	let s = raw(&governed_root(), mg, mc, &enroll, 3, 0).0;
	let v: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	let grant: [u8; 337] = unhex(v["grant"]["bytes"].as_str().unwrap())
		.try_into()
		.unwrap();
	let cmd = Command2::AdmissionPermission(grant);
	let (after, r) = via_c(&s, mg, mc, &cmd, 4).unwrap();
	let (raw_after, raw_r) = raw(&s, mg, mc, &cmd, 4, 0);
	assert!(r == raw_r && matches!(r, ApplyResult2::Permission { .. }));
	assert_eq!(bytes(&after), bytes(&raw_after));
	// Tampered signature byte, and a statement by another signer: C's verdict is a consumed
	// Denied(8); the raw gate with an always-authorized token reports its own statement denial.
	let mut tampered = grant;
	tampered[336] ^= 1;
	let other: [u8; 337] = unhex(v["other_signer"]["bytes"].as_str().unwrap())
		.try_into()
		.unwrap();
	for bad in [tampered, other] {
		let cmd = Command2::AdmissionPermission(bad);
		consumed(&s, via_c(&s, mg, mc, &cmd, 4), 8, 4);
		// The gate's own statement check agrees: also a consumed Denied(8).
		assert!(matches!(
			raw(&s, mg, mc, &cmd, 4, 0).1,
			ApplyResult2::Denied { reason: 8, .. }
		));
	}
	// An install failure (the pin of another manifest record): the consumer refuses as well.
	let mut other_record = record();
	other_record.identity.genesis_fingerprint = [0x77; 32];
	let (ce, e) = entry_of(mg, mc, &cmd, 4);
	assert_eq!(
		advance_verified2(&s, &tuple_for(other_record), &ce, &e, &cmd, None).err(),
		Some(VerifyError2::Consumer(RootError2::TrustMismatch))
	);
}

#[test]
fn reserve_signature_key_and_scope_cases() {
	let s = standard();
	let (after, r) = via_c(&s, g(), c(), &reserve(), 7).unwrap();
	let (raw_after, raw_r) = raw(&s, g(), c(), &reserve(), 7, 0);
	// A new reservation: Parent { new_effect: false } (only an accepted terminal is a new effect)
	// and exactly one more parent, as the raw consumer gives.
	assert!(
		r == raw_r
			&& matches!(
				r,
				ApplyResult2::Parent {
					new_effect: false,
					..
				}
			),
		"C {} raw {}",
		describe(&r),
		describe(&raw_r)
	);
	assert_eq!(parents(&after), parents(&s) + 1);
	assert_eq!(bytes(&after), bytes(&raw_after));
	// Wrong enrolled key; unknown authority; a tampered claim; revoked authority.
	let wrong_key = kat_root(c(), d32(kat("public_test_proof_key")), true, false);
	consumed(&wrong_key, via_c(&wrong_key, g(), c(), &reserve(), 7), 8, 7);
	let unknown = kat_root(c(), [0; 32], false, false);
	consumed(&unknown, via_c(&unknown, g(), c(), &reserve(), 7), 8, 7);
	let mut cl = claim();
	cl.accounting_delta += 1;
	consumed(&s, via_c(&s, g(), c(), &Command2::Reserve(cl), 7), 8, 7);
	let revoked = raw(&s, g(), c(), &revoke(), 7, 0).0;
	consumed(&revoked, via_c(&revoked, g(), c(), &reserve(), 8), 8, 8);
	// A valid signature on a non-latest original: historical is never set, so Denied(9).
	let renewed = kat_root(c(), d32(kat("public_key")), true, true);
	let (_, r) = via_c(&renewed, g(), c(), &reserve(), 7).unwrap();
	assert!(matches!(r, ApplyResult2::Denied { reason: 9, .. }));
}

#[test]
fn finalize_signature_scope_and_quorum_cases() {
	let s = reserved(false);
	// The KAT attestation: equal to the raw consumer given valid evidence, never Denied(8).
	let cmd = finalize(holder());
	let (after, r) = via_c(&s, g(), c(), &cmd, 8).unwrap();
	let (raw_after, raw_r) = raw(&s, g(), c(), &cmd, 8, 1);
	assert!(r == raw_r && !matches!(r, ApplyResult2::Denied { reason: 8, .. }));
	assert_eq!(bytes(&after), bytes(&raw_after));
	// A validly re-signed attestation whose acks cannot meet the quorum: the consumer's own
	// evidence result (not Accepted), distinct from an invalid signature.
	let mut thin = holder();
	let holder_node = NodeRef2 {
		node: claim().original.lease.holder,
		incarnation: claim().original.incarnation,
	};
	for child in &mut thin.children {
		child.nodes.retain(|n| *n != holder_node);
	}
	let cmd = finalize(resigned(thin));
	let (_, r) = via_c(&s, g(), c(), &cmd, 8).unwrap();
	assert!(r == raw(&s, g(), c(), &cmd, 8, 1).1);
	assert!(!matches!(
		r,
		ApplyResult2::Denied { .. }
			| ApplyResult2::Parent {
				new_effect: true,
				..
			}
	));
	// Bad signature, scope mismatch, wrong digest, wrong reservation, missing parent: Denied(8).
	let mut bad = holder();
	bad.signature[0] ^= 1;
	let mut scope = holder();
	scope.claim_digest[0] ^= 1;
	for cmd in [finalize(bad), finalize(scope)] {
		consumed(&s, via_c(&s, g(), c(), &cmd, 8), 8, 8);
	}
	let base = finalize(holder());
	let Command2::Finalize {
		identity,
		digest,
		reservation,
		attestation,
	} = base
	else {
		unreachable!()
	};
	for cmd in [
		Command2::Finalize {
			identity,
			digest: [0xee; 32],
			reservation,
			attestation: attestation.clone(),
		},
		Command2::Finalize {
			identity,
			digest,
			reservation: l(6),
			attestation: attestation.clone(),
		},
	] {
		consumed(&s, via_c(&s, g(), c(), &cmd, 8), 8, 8);
	}
	let no_parent = standard();
	let cmd = finalize(holder());
	consumed(&no_parent, via_c(&no_parent, g(), c(), &cmd, 7), 8, 7);
}

/// C r2 §4: the conservative outer policy, pinned against the raw consumer's semantics.
#[test]
fn outer_authorization_limitations_are_pinned() {
	// (a) Revoked holder, pending parent, committed revocation closure.
	let s = reserved(true);
	let cmd = finalize(holder());
	consumed(&s, via_c(&s, g(), c(), &cmd, 9), 8, 9);
	assert!(matches!(
		raw(&s, g(), c(), &cmd, 9, 1).1,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	// (b) A terminal parent (FIXTURE: the raw Refused terminal above) plus an invalid proof.
	let terminal = raw(&s, g(), c(), &cmd, 9, 1).0;
	let mut bad = holder();
	bad.signature[1] ^= 1;
	let cmd = finalize(bad);
	consumed(&terminal, via_c(&terminal, g(), c(), &cmd, 10), 8, 10);
	assert!(matches!(
		raw(&terminal, g(), c(), &cmd, 10, 2).1,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	// (c) Reserve semantic replay after a FIXTURE revocation.
	consumed(&s, via_c(&s, g(), c(), &reserve(), 9), 8, 9);
	assert!(matches!(
		raw(&s, g(), c(), &reserve(), 9, 0).1,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	// (d) Exact covered-index replay of an entry whose verification fails: AlreadyProcessed,
	// reached before authorization, whole state unchanged.
	let mut cl = claim();
	cl.accounting_delta += 1;
	let bad = Command2::Reserve(cl);
	let denied = via_c(&standard(), g(), c(), &bad, 7).unwrap().0;
	let (again, r) = via_c(&denied, g(), c(), &bad, 7).unwrap();
	assert!(r == ApplyResult2::AlreadyProcessed);
	assert_eq!(bytes(&again), bytes(&denied));
}

#[test]
fn context_change_is_seen() {
	let s = standard();
	let (reserved, r) = via_c(&s, g(), c(), &reserve(), 7).unwrap();
	assert!(matches!(r, ApplyResult2::Parent { .. }), "{}", describe(&r));
	assert_eq!(parents(&reserved), parents(&s) + 1);

	// FIXTURE revocation at 7, then the same Reserve through C at 8.
	let revoked = raw(&s, g(), c(), &revoke(), 7, 0).0;
	consumed(&revoked, via_c(&revoked, g(), c(), &reserve(), 8), 8, 8);
}

#[test]
fn committed_and_entry_binding() {
	let s = standard();
	let cmd = reserve();
	let e = TrustedEntry2::fixture(g(), c(), l(7), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	for wrong in [
		CommittedEntry2::fixture(at(1, 1, 7), fp, EntryClass2::Blank),
		CommittedEntry2::fixture(at(1, 1, 7), fp, EntryClass2::NonRoot2),
		CommittedEntry2::fixture(at(1, 1, 7), fp, EntryClass2::Bootstrap(d)),
		CommittedEntry2::fixture(at(1, 1, 7), fp, EntryClass2::OtherRoot2([0xdd; 32])),
		CommittedEntry2::fixture(at(1, 1, 8), fp, EntryClass2::OtherRoot2(d)),
		CommittedEntry2::fixture(at(2, 1, 7), fp, EntryClass2::OtherRoot2(d)),
		CommittedEntry2::fixture(at(1, 1, 7), [0xcc; 32], EntryClass2::OtherRoot2(d)),
	] {
		assert_eq!(
			advance_verified2(&s, &tuple(), &wrong, &e, &cmd, None).err(),
			Some(VerifyError2::CommittedMismatch)
		);
	}
	// Bootstrap: its binding is bootstrap2's.
	let m = manifest_prime();
	let boot = governed(0, None, 137);
	let (ce, e) = bootstrap_inputs(m.generation, m.cluster, &boot);
	let (pos, id, class) = ce.entry_fields();
	for wrong in [
		CommittedEntry2::fixture(pos, id, EntryClass2::OtherRoot2([1; 32])),
		CommittedEntry2::fixture(at(1, 1, 3), id, class),
		CommittedEntry2::fixture(pos, [0xcc; 32], class),
	] {
		assert_eq!(
			advance_verified2(
				&pre_root(d32(sv("genesis"))),
				&tuple(),
				&wrong,
				&e,
				&boot,
				None
			)
			.err(),
			Some(VerifyError2::Consumer(RootError2::TrustMismatch))
		);
	}
}

#[test]
fn root_mismatch_is_separate_from_foreign_signature() {
	let s = standard();
	assert_eq!(
		via_c(&s, [0x5c; 16], c(), &reserve(), 7).err(),
		Some(VerifyError2::RootMismatch)
	);
	assert_eq!(
		via_c(&s, g(), [0x5d; 16], &reserve(), 7).err(),
		Some(VerifyError2::RootMismatch)
	);
	// A root of another cluster, coherent entries: the KAT signature (made for c()) fails there.
	let other = [0x34; 16];
	let foreign = kat_root(other, d32(kat("public_key")), true, false);
	consumed(&foreign, via_c(&foreign, g(), other, &reserve(), 7), 8, 7);
}

#[test]
fn unsupported_not_applicable_and_precedence() {
	let s = standard();
	let before = bytes(&s);
	let a = claim().authority;
	let o = claim().original;
	let unsupported = [
		(
			Command2::EnrollLeaseIssuer {
				node: 1,
				key: [0; 32],
			},
			UnsupportedKind2::EnrollLeaseIssuer,
		),
		(
			Command2::RevokeLeaseIssuer { node: 1 },
			UnsupportedKind2::RevokeLeaseIssuer,
		),
		(
			Command2::EnrollSelector(claim().manifest.binding()),
			UnsupportedKind2::EnrollSelector,
		),
		(
			Command2::EnrollAuthority {
				authority: a,
				node: NodeRef2 {
					node: 1,
					incarnation: [1; 16],
				},
				key: [1; 32],
			},
			UnsupportedKind2::EnrollAuthority,
		),
		(
			Command2::RevokeAuthority(a),
			UnsupportedKind2::RevokeAuthority,
		),
		(
			Command2::Grant {
				lineage: o.lineage,
				incarnation: o.incarnation,
				lease: o.lease.clone(),
				predecessor: None,
			},
			UnsupportedKind2::Grant,
		),
		(
			Command2::Release {
				lineage: o.lineage,
				latest: o.reference().unwrap(),
			},
			UnsupportedKind2::Release,
		),
		(
			Command2::Cut {
				expected: 1,
				next: 2,
				configuration: [2; 32],
			},
			UnsupportedKind2::Cut,
		),
		(
			Command2::Resolve {
				identity: claim().manifest.identity,
				digest: [3; 32],
				reservation: l(7),
				cause: 1,
				anchor: l(5),
			},
			UnsupportedKind2::Resolve,
		),
	];
	for (cmd, _legacy_kind) in unsupported {
		// Historical name retained: S3 now accepts these kinds, so digest refuses before missing sidecar.
		let (ce, e) = entry_of(g(), c(), &reserve(), 7);
		assert_eq!(
			advance_verified2(&s, &tuple(), &ce, &e, &cmd, None).err(),
			Some(VerifyError2::DigestMismatch)
		);
		let (ce, e) = entry_of(g(), c(), &cmd, 7);
		assert_eq!(
			advance_verified2(&s, &tuple(), &ce, &e, &cmd, None).err(),
			Some(VerifyError2::MissingSidecar)
		);
	}
	let cover = Command2::Cover {
		kind: 0,
		external: [1; 32],
	};
	assert_eq!(
		via_c(&s, g(), c(), &cover, 7).err(),
		Some(VerifyError2::NotApplicable)
	);
	assert_eq!(bytes(&s), before, "refusals leave the state unchanged");
	// Digest before committed binding.
	let (_, e_other) = entry_of(g(), c(), &revoke(), 7);
	let blank = CommittedEntry2::fixture(at(1, 1, 7), [0; 32], EntryClass2::Blank);
	assert_eq!(
		advance_verified2(&s, &tuple(), &blank, &e_other, &reserve(), None).err(),
		Some(VerifyError2::DigestMismatch)
	);
	// Committed binding before state kind (rooted kinds), state kind before root.
	let pre = pre_root([0x11; 32]);
	let (_, e) = entry_of(g(), c(), &reserve(), 7);
	assert_eq!(
		advance_verified2(&pre, &tuple(), &blank, &e, &reserve(), None).err(),
		Some(VerifyError2::CommittedMismatch)
	);
	assert_eq!(
		via_c(&pre, [0x5c; 16], c(), &reserve(), 7).err(),
		Some(VerifyError2::StateKind)
	);
}

#[test]
fn vectors_match_reference() {
	let cl = claim();
	let h = holder();
	assert_eq!(vv("public_key"), kat("public_key"));
	assert_eq!(
		admission_preimage2(&cl, c()).unwrap(),
		vv("admission_preimage")
	);
	assert_eq!(cl.signature.to_vec(), vv("admission_signature"));
	assert_eq!(
		holder_preimage2(&cl, c(), &h).unwrap(),
		vv("holder_preimage")
	);
	assert_eq!(h.signature.to_vec(), vv("holder_signature"));
	let v: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	let grant: [u8; 337] = unhex(v["grant"]["bytes"].as_str().unwrap())
		.try_into()
		.unwrap();
	assert_eq!(
		admin_statement_preimage2(&grant).unwrap(),
		vv("statement_preimage")
	);
	assert_eq!(grant[273..].to_vec(), vv("statement_signature"));
	assert_eq!(sv("admin_public"), vv("statement_admin_public"));
}
