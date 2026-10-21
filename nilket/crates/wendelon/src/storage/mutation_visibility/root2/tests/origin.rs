use super::*;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;

const G: [u8; 16] = [0xa1; 16];
const C: [u8; 16] = [0xc2; 16];
const GENESIS_ID: Digest2 = [0x11; 32];

fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
fn committed(p: EntryPosition2, id: u8, class: EntryClass2) -> CommittedEntry2 {
	CommittedEntry2::fixture(p, [id; 32], class)
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("origin_vectors.json")).unwrap()
}
fn bytes(name: &str) -> Vec<u8> {
	let h = vectors()[name].as_str().unwrap().to_string();
	(0..h.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
		.collect()
}
fn genesis() -> MutationAuthorityState2 {
	let g = CommittedEntry2::fixture(at(0, 0, 0), GENESIS_ID, EntryClass2::Membership);
	let (s, step) = advance_pre_root2(&empty_authority2(), &g).unwrap();
	assert_eq!(step, PreRootStep2::Genesis);
	s
}
fn scanned(n: u64) -> MutationAuthorityState2 {
	let mut s = genesis();
	for i in 1..=n {
		s = advance_pre_root2(&s, &committed(at(1, 1, i), i as u8, EntryClass2::NonRoot2))
			.unwrap()
			.0;
	}
	s
}
fn bootstrap_command() -> Command2 {
	Command2::Bootstrap {
		epoch: 1,
		control: [0x9f; 32],
		length: 241,
	}
}
// Produces the trusted inputs for a Bootstrap at `l` and the committed token bound to them.
fn bootstrap_inputs(l: Log2) -> (CommittedEntry2, TrustedEntry2, VerifiedCommand2, Command2) {
	let cmd = bootstrap_command();
	let e = TrustedEntry2::fixture(G, C, l, &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let v = VerifiedCommand2::fixture(G, C, &cmd, &[], 0).unwrap();
	let ce = CommittedEntry2::fixture(at(l.term, l.node, l.index), fp, EntryClass2::Bootstrap(d));
	(ce, e, v, cmd)
}
fn rooted() -> MutationAuthorityState2 {
	let s = scanned(1);
	let (ce, e, v, cmd) = bootstrap_inputs(l(2));
	let (r, result) = bootstrap2(&s, &ce, &e, &v, &cmd).unwrap();
	assert!(matches!(result, ApplyResult2::Registered(x) if x == l(2)));
	r
}
fn encoded(s: &MutationAuthorityState2) -> Vec<u8> {
	encode_authority2(s).unwrap()
}

#[test]
fn independent_wrapper_goldens_round_trip_and_constructed_states_match() {
	assert_eq!(encoded(&empty_authority2()), bytes("empty"));
	assert_eq!(encoded(&genesis()), bytes("genesis_only"));
	for (name, tag) in [
		("empty", 0),
		("genesis_only", 1),
		("pre_root", 1),
		("p3_root", 2),
	] {
		let b = bytes(name);
		let u = UnverifiedAuthority2::decode(&b).unwrap();
		assert_eq!(u.tag_for_test(), tag, "{name}");
		assert_eq!(u.encode().unwrap(), b, "{name}");
	}
	// The nested root is the committed golden (its profile-3 twin); its scalars feed the origin
	// check, and the p3_ wrapper holds exactly it.
	let v = vectors();
	let root = fixture("root_bootstrap_only");
	let (bootstrap, covered, fp, command) = UnverifiedRoot2::decode(&root)
		.unwrap()
		.origin_anchor_fields2();
	assert_eq!(bootstrap, covered);
	assert_eq!(
		bootstrap.index,
		v["root_bootstrap"]["index"].as_u64().unwrap()
	);
	let hex = |d: &[u8]| d.iter().map(|b| format!("{b:02x}")).collect::<String>();
	assert_eq!(hex(&fp), v["p3_root_fingerprint"].as_str().unwrap());
	assert_eq!(hex(&command), v["p3_root_command"].as_str().unwrap());
	let wrapper = bytes("p3_root");
	let u = UnverifiedAuthority2::decode(&wrapper).unwrap();
	assert_eq!(u.tag_for_test(), 2);
	assert_eq!(u.encode().unwrap(), wrapper);
	let n = v["p3_nested_root_len"].as_u64().unwrap() as usize;
	assert_eq!(n, root.len());
	assert_eq!(
		wrapper[wrapper.len() - 32 - n..wrapper.len() - 32],
		root[..]
	);
}
#[test]
fn pre_root_table_rows() {
	let e = empty_authority2();
	// Empty: only (0,0,0) Membership.
	assert_eq!(
		advance_pre_root2(&e, &committed(at(0, 0, 0), 1, EntryClass2::Blank)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		advance_pre_root2(&e, &committed(at(1, 0, 0), 1, EntryClass2::Membership)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		advance_pre_root2(&e, &committed(at(0, 3, 0), 1, EntryClass2::Membership)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		advance_pre_root2(&e, &committed(at(1, 1, 1), 1, EntryClass2::Membership)).err(),
		Some(RootError2::LineageGap)
	);
	let g = genesis();
	assert!(coverage2(&g) == Some(at(0, 0, 0)));
	// Repeated genesis is a no-op; a substituted genesis is refused.
	let (same, step) = advance_pre_root2(
		&g,
		&CommittedEntry2::fixture(at(0, 0, 0), GENESIS_ID, EntryClass2::Membership),
	)
	.unwrap();
	assert_eq!(step, PreRootStep2::AlreadyProcessed);
	assert_eq!(encoded(&same), encoded(&g));
	assert_eq!(
		advance_pre_root2(&g, &committed(at(0, 0, 0), 2, EntryClass2::Membership)).err(),
		Some(RootError2::TrustMismatch)
	);
	// Gaps and invalid later positions.
	assert_eq!(
		advance_pre_root2(&g, &committed(at(1, 1, 2), 1, EntryClass2::Blank)).err(),
		Some(RootError2::LineageGap)
	);
	assert_eq!(
		advance_pre_root2(&g, &committed(at(0, 1, 1), 1, EntryClass2::Blank)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		advance_pre_root2(&g, &committed(at(1, 0, 1), 1, EntryClass2::Blank)).err(),
		Some(RootError2::TrustMismatch)
	);
	// Successful scan rows for each scanned class (NonRoot2 is exercised by `scanned`).
	let (b, step) = advance_pre_root2(&g, &committed(at(1, 1, 1), 1, EntryClass2::Blank)).unwrap();
	assert_eq!(step, PreRootStep2::Scanned);
	assert!(coverage2(&b) == Some(at(1, 1, 1)));
	let (m, step) =
		advance_pre_root2(&b, &committed(at(1, 1, 2), 2, EntryClass2::Membership)).unwrap();
	assert_eq!(step, PreRootStep2::Scanned);
	assert!(coverage2(&m) == Some(at(1, 1, 2)));
	assert!(m.malformed_for_test() == Some((0, None)));
	// Non-genesis same-index duplicate (addendum A).
	let s = scanned(3);
	let (dup, step) =
		advance_pre_root2(&s, &committed(at(1, 1, 3), 3, EntryClass2::NonRoot2)).unwrap();
	assert_eq!(step, PreRootStep2::AlreadyProcessed);
	assert_eq!(encoded(&dup), encoded(&s));
	assert_eq!(
		advance_pre_root2(&s, &committed(at(1, 1, 3), 9, EntryClass2::NonRoot2)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		advance_pre_root2(&s, &committed(at(2, 1, 3), 3, EntryClass2::NonRoot2)).err(),
		Some(RootError2::TrustMismatch)
	);
	assert_eq!(
		advance_pre_root2(&s, &committed(at(1, 1, 1), 1, EntryClass2::NonRoot2)).err(),
		Some(RootError2::LineageGap)
	);
	// Bootstrap class takes the other route (addendum B).
	assert_eq!(
		advance_pre_root2(
			&s,
			&committed(at(1, 1, 4), 4, EntryClass2::Bootstrap([1; 32]))
		)
		.err(),
		Some(RootError2::TrustMismatch)
	);
	// Borrowed input unchanged after every error above.
	assert!(coverage2(&s) == Some(at(1, 1, 3)));
	assert!(coverage2(&e) == None);
}
#[test]
fn malformed_candidates_are_counted_once_and_saturate() {
	let s = scanned(1);
	let (s, step) =
		advance_pre_root2(&s, &committed(at(1, 1, 2), 2, EntryClass2::MalformedRoot2)).unwrap();
	assert_eq!(step, PreRootStep2::Malformed);
	let (s, step) = advance_pre_root2(
		&s,
		&committed(at(1, 1, 3), 3, EntryClass2::OtherRoot2([5; 32])),
	)
	.unwrap();
	assert_eq!(step, PreRootStep2::Malformed);
	assert!(s.malformed_for_test() == Some((2, Some(at(1, 1, 2)))));
	let full = pre_root_with_malformed_for_test(u32::MAX, GENESIS_ID);
	let (sat, _) = advance_pre_root2(
		&full,
		&committed(at(1, 1, 2), 2, EntryClass2::MalformedRoot2),
	)
	.unwrap();
	assert!(sat.malformed_for_test() == Some((u32::MAX, Some(at(1, 1, 1)))));
	// The count is carried into Origin2 at bootstrap.
	let (ce, e, v, cmd) = bootstrap_inputs(l(4));
	let (root, _) = bootstrap2(&s, &ce, &e, &v, &cmd).unwrap();
	assert!(root.malformed_for_test() == Some((2, None)));
}
#[test]
fn bootstrap_bindings_refuse_every_mismatch_and_leave_state_unchanged() {
	let s = scanned(1);
	let before = encoded(&s);
	let (ce, e, v, cmd) = bootstrap_inputs(l(2));
	let (_, _, _, fp, d) = e.fields();
	let cases = [
		CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::OtherRoot2(d)),
		CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap([0; 32])),
		CommittedEntry2::fixture(at(2, 1, 2), fp, EntryClass2::Bootstrap(d)),
		CommittedEntry2::fixture(at(1, 1, 2), [0; 32], EntryClass2::Bootstrap(d)),
	];
	for c in &cases {
		assert_eq!(
			bootstrap2(&s, c, &e, &v, &cmd).err(),
			Some(RootError2::TrustMismatch)
		);
	}
	// Non-adjacent index.
	let (ce3, e3, v3, cmd3) = bootstrap_inputs(l(3));
	assert_eq!(
		bootstrap2(&s, &ce3, &e3, &v3, &cmd3).err(),
		Some(RootError2::TrustMismatch)
	);
	// Supplied command must itself be Bootstrap.
	let cover = Command2::Cover {
		kind: 1,
		external: [3; 32],
	};
	assert_eq!(
		bootstrap2(&s, &ce, &e, &v, &cover).err(),
		Some(RootError2::TrustMismatch)
	);
	// Unauthorized verification is refused by the existing reducer.
	let unauthorized = VerifiedCommand2::fixture(G, C, &cmd, &[], 0)
		.unwrap()
		.unauthorized();
	assert_eq!(
		bootstrap2(&s, &ce, &e, &unauthorized, &cmd).err(),
		Some(RootError2::TrustMismatch)
	);
	// Node 0 reaches valid_log inside the reducer: Codec, not a counted candidate.
	let zero = Log2 {
		term: 1,
		node: 0,
		index: 2,
	};
	let (cz, ez, vz, cmdz) = bootstrap_inputs(zero);
	assert_eq!(
		bootstrap2(&s, &cz, &ez, &vz, &cmdz).err(),
		Some(RootError2::Codec)
	);
	assert_eq!(encoded(&s), before);
	assert_eq!(
		bootstrap2(&empty_authority2(), &ce, &e, &v, &cmd).err(),
		Some(RootError2::TrustMismatch)
	);
}
#[test]
fn established_root_keeps_reducer_semantics_and_origin() {
	let r = rooted();
	assert!(r.is_root_for_test());
	assert!(coverage2(&r) == Some(at(1, 1, 2)));
	let origin_bytes = encoded(&r);
	// Exact duplicate of the covered Bootstrap entry.
	let (ce, e, v, cmd) = bootstrap_inputs(l(2));
	let _ = ce;
	let (dup, result) = advance_root2(&r, &e, &v, &cmd).unwrap();
	assert!(matches!(result, ApplyResult2::AlreadyProcessed));
	assert_eq!(encoded(&dup), origin_bytes);
	// Same index, different identity.
	let mut changed = TrustedEntry2::fixture(G, C, l(2), &cmd).unwrap();
	changed.change_fingerprint();
	assert_eq!(
		advance_root2(&r, &changed, &v, &cmd).err(),
		Some(RootError2::TrustMismatch)
	);
	// Gap.
	let cover = Command2::Cover {
		kind: 0,
		external: [4; 32],
	};
	let ve = VerifiedCommand2::fixture(G, C, &cover, &[], 0).unwrap();
	let gap = TrustedEntry2::fixture(G, C, l(4), &cover).unwrap();
	assert_eq!(
		advance_root2(&r, &gap, &ve, &cover).err(),
		Some(RootError2::LineageGap)
	);
	// Cover kinds 0/1/2 advance coverage with Covered.
	let mut s = r;
	for (i, kind) in [(3u64, 0u8), (4, 1), (5, 2)] {
		let c = Command2::Cover {
			kind,
			external: [i as u8; 32],
		};
		let te = TrustedEntry2::fixture(G, C, l(i), &c).unwrap();
		let tv = VerifiedCommand2::fixture(G, C, &c, &[], 0).unwrap();
		let (n, result) = advance_root2(&s, &te, &tv, &c).unwrap();
		assert!(matches!(result, ApplyResult2::Covered));
		assert!(coverage2(&n) == Some(at(1, 1, i)));
		s = n;
	}
	// Unauthorized Cover: existing Denied(8), coverage still advances.
	let c = Command2::Cover {
		kind: 1,
		external: [6; 32],
	};
	let te = TrustedEntry2::fixture(G, C, l(6), &c).unwrap();
	let tv = VerifiedCommand2::fixture(G, C, &c, &[], 0)
		.unwrap()
		.unauthorized();
	let (n, result) = advance_root2(&s, &te, &tv, &c).unwrap();
	assert!(matches!(result, ApplyResult2::Denied { reason: 8, .. }));
	assert!(coverage2(&n) == Some(at(1, 1, 6)));
	// A later distinct Bootstrap: Denied(1), coverage advances, wrapper still decodes.
	let late = Command2::Bootstrap {
		epoch: 2,
		control: [1; 32],
		length: 241,
	};
	let te = TrustedEntry2::fixture(G, C, l(7), &late).unwrap();
	let tv = VerifiedCommand2::fixture(G, C, &late, &[], 0).unwrap();
	let (n2, result) = advance_root2(&n, &te, &tv, &late).unwrap();
	assert!(matches!(result, ApplyResult2::Denied { reason: 1, .. }));
	assert!(coverage2(&n2) == Some(at(1, 1, 7)));
	let decoded = UnverifiedAuthority2::decode(&encoded(&n2)).unwrap();
	assert_eq!(decoded.tag_for_test(), 2);
	// Pre-root transitions refuse an established root.
	assert_eq!(
		advance_pre_root2(&n2, &committed(at(1, 1, 8), 8, EntryClass2::Blank)).err(),
		Some(RootError2::TrustMismatch)
	);
}
#[test]
fn wrapper_negatives() {
	let reseal = |body: &[u8]| envelope(b"VISMAS02", body, 1 << 28).unwrap();
	let body = |name: &str| bytes(name)[16..bytes(name).len() - 32].to_vec();
	let pre = body("pre_root");
	let root = body("p3_root");
	let refuse = |b: &[u8]| UnverifiedAuthority2::decode(b).err().expect("refused");
	// Unknown tag, trailing byte, short PreRoot.
	assert_eq!(refuse(&reseal(&[3])), RootError2::Codec);
	assert_eq!(refuse(&reseal(&[0, 0])), RootError2::Codec);
	let mut long = pre.clone();
	long.push(0);
	assert_eq!(refuse(&reseal(&long)), RootError2::Codec);
	assert_eq!(refuse(&reseal(&pre[..pre.len() - 1])), RootError2::Codec);
	// Presence/zero mismatch, first_malformed above scan, first_malformed at 0, count without presence.
	let mut p = pre.clone();
	p[89] = 0; // presence 0 with a nonzero position
	assert_eq!(refuse(&reseal(&p)), RootError2::Codec);
	let mut p = pre.clone();
	p[90..110].copy_from_slice(
		&[
			[3u8, 0, 0, 0, 0, 0, 0, 0].as_slice(),
			&[2, 0, 0, 0],
			&[9, 0, 0, 0, 0, 0, 0, 0],
		]
		.concat(),
	);
	assert_eq!(refuse(&reseal(&p)), RootError2::RootInvariant);
	let mut p = pre.clone();
	p[90..110].fill(0);
	assert_eq!(refuse(&reseal(&p)), RootError2::RootInvariant);
	let mut p = pre.clone();
	p[85..89].copy_from_slice(&0u32.to_le_bytes());
	assert_eq!(refuse(&reseal(&p)), RootError2::RootInvariant);
	// Index-0 scan whose last fingerprint differs from genesis.
	let mut g = body("genesis_only");
	g[53] ^= 1;
	assert_eq!(refuse(&reseal(&g)), RootError2::RootInvariant);
	// Root: mismatched origin pair while covered == bootstrap; nested length over and short.
	let mut r = root.clone();
	r[33] ^= 1; // bootstrap fingerprint
	assert_eq!(refuse(&reseal(&r)), RootError2::RootInvariant);
	let mut r = root.clone();
	r[101..105].copy_from_slice(&u32::MAX.to_le_bytes());
	assert_eq!(refuse(&reseal(&r)), RootError2::Limit);
	let mut r = root.clone();
	let n = u32::from_le_bytes(r[101..105].try_into().unwrap());
	r[101..105].copy_from_slice(&(n - 1).to_le_bytes());
	assert_eq!(refuse(&reseal(&r)), RootError2::Codec);
	// A nested root the existing decoder rejects.
	let mut r = root.clone();
	let last = r.len() - 1;
	r[last] ^= 1;
	assert_eq!(refuse(&reseal(&r)), RootError2::Codec);
	// Corrupted outer checksum.
	let mut whole = bytes("empty");
	let end = whole.len() - 1;
	whole[end] ^= 1;
	assert_eq!(refuse(&whole), RootError2::Codec);
}
#[test]
fn next_index_overflow_is_refused_by_both_routes_without_changing_state() {
	let top = at(1, 1, u64::MAX);
	let s = pre_root_scanned_through_for_test(top, [0x33; 32], GENESIS_ID);
	let before = encoded(&s);
	assert!(coverage2(&s) == Some(top));
	// Any index other than the scanned one needs scanned + 1, which overflows.
	for class in [
		EntryClass2::Blank,
		EntryClass2::NonRoot2,
		EntryClass2::MalformedRoot2,
	] {
		assert_eq!(
			advance_pre_root2(&s, &committed(at(1, 1, 5), 5, class)).err(),
			Some(RootError2::Overflow)
		);
	}
	// The scanned entry itself is still recognised as a duplicate.
	let (same, step) = advance_pre_root2(
		&s,
		&CommittedEntry2::fixture(top, [0x33; 32], EntryClass2::NonRoot2),
	)
	.unwrap();
	assert_eq!(step, PreRootStep2::AlreadyProcessed);
	assert_eq!(encoded(&same), before);
	let (ce, e, v, cmd) = bootstrap_inputs(l(5));
	assert_eq!(
		bootstrap2(&s, &ce, &e, &v, &cmd).err(),
		Some(RootError2::Overflow)
	);
	assert_eq!(encoded(&s), before);
}
