//! Stage A1: deterministic post-root coverage. Coverage is derived from one genuine committed
//! entry after the Bootstrap; it advances the tip through the reducer's own checks and cannot
//! reach any effect.
use super::*;
use crate::consensus::{WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::committed::{CommittedSource2, committed_entry2};
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::incremental::cached_capacity;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::recovery::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership};
use std::collections::BTreeMap;

const G: [u8; 16] = [0xa1; 16];
const C: [u8; 16] = [0xc2; 16];
const GENESIS_ID: Digest2 = [0x11; 32];

fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
fn log2(p: EntryPosition2) -> Log2 {
	Log2 {
		term: p.term,
		node: p.node,
		index: p.index,
	}
}
fn committed(index: u64, id: u8, class: EntryClass2) -> CommittedEntry2 {
	CommittedEntry2::fixture(at(1, 1, index), [id; 32], class)
}
fn genesis() -> MutationAuthorityState2 {
	let g = CommittedEntry2::fixture(at(0, 0, 0), GENESIS_ID, EntryClass2::Membership);
	advance_pre_root2(&empty_authority2(), &g).unwrap().0
}
fn scanned() -> MutationAuthorityState2 {
	advance_pre_root2(&genesis(), &committed(1, 1, EntryClass2::NonRoot2))
		.unwrap()
		.0
}
/// Genesis at 0, one scanned entry at 1, the Bootstrap at (1,1,2).
fn rooted() -> MutationAuthorityState2 {
	let cmd = Command2::Bootstrap {
		epoch: 1,
		control: [0x9f; 32],
		length: 241,
	};
	let e = TrustedEntry2::fixture(G, C, l(2), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let v = VerifiedCommand2::fixture(G, C, &cmd, &[], 0).unwrap();
	let ce = CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap(d));
	bootstrap2(&scanned(), &ce, &e, &v, &cmd).unwrap().0
}
fn root_of(s: &MutationAuthorityState2) -> &Root2 {
	root_and_origin2(s).expect("rooted").0
}
fn tip(s: &MutationAuthorityState2) -> (Log2, Digest2, Digest2) {
	let st = &root_of(s).state;
	(st.covered, st.fingerprint, st.command_digest)
}
fn derived_digest(kind: u8, identity: Digest2) -> Digest2 {
	Command2::Cover {
		kind,
		external: identity,
	}
	.digest(G, C)
	.unwrap()
}
fn covered_by(s: &MutationAuthorityState2, e: &CommittedEntry2) -> MutationAuthorityState2 {
	let (next, result) = advance_cover2(s, e).unwrap();
	assert!(result == ApplyResult2::Covered);
	next
}
fn limits(l: Limits2) -> [usize; 8] {
	[
		l.slots,
		l.parent,
		l.history,
		l.registry,
		l.lineages,
		l.cuts,
		l.registrations,
		l.permissions,
	]
}
/// Whole-state equality: the encoded authority plus what the encoding omits.
fn assert_same_state(a: &MutationAuthorityState2, b: &MutationAuthorityState2) {
	assert_eq!(encode_authority2(a).unwrap(), encode_authority2(b).unwrap());
	assert!(root_of(a).state.derived == root_of(b).state.derived);
	assert_eq!(limits(root_of(a).limits), limits(root_of(b).limits));
	assert!(a.malformed_for_test() == b.malformed_for_test());
	assert!(root_and_origin2(a).unwrap().1 == root_and_origin2(b).unwrap().1);
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("cover_vectors.json")).unwrap()
}
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}

#[test]
fn derived_cover_matches_vectors() {
	let cases = vectors()["cases"].as_array().unwrap().clone();
	assert_eq!(cases.len(), 6);
	for case in &cases {
		let g: [u8; 16] = unhex(case["generation"].as_str().unwrap())
			.try_into()
			.unwrap();
		let c: [u8; 16] = unhex(case["cluster"].as_str().unwrap()).try_into().unwrap();
		let kind = case["kind"].as_u64().unwrap() as u8;
		let external: Digest2 = unhex(case["external"].as_str().unwrap())
			.try_into()
			.unwrap();
		let command = Command2::Cover { kind, external };
		assert_eq!(
			command.payload(g, c).unwrap(),
			unhex(case["payload"].as_str().unwrap()),
			"{}",
			case["name"]
		);
		assert_eq!(
			command.digest(g, c).unwrap().to_vec(),
			unhex(case["digest"].as_str().unwrap()),
			"{}",
			case["name"]
		);
		// The rooted fixture's (generation, cluster) are the vectors': the derived tip matches.
		assert_eq!((g, c), (G, C));
		let class = match kind {
			0 => EntryClass2::Blank,
			1 => EntryClass2::Membership,
			_ => EntryClass2::NonRoot2,
		};
		let after = covered_by(&rooted(), &committed(3, external[0], class));
		assert_eq!(
			tip(&after).2.to_vec(),
			unhex(case["digest"].as_str().unwrap())
		);
		assert_eq!(
			tip(&after).1,
			external,
			"the fingerprint is the entry identity"
		);
	}
}

#[test]
fn post_bootstrap_entries_cover_contiguously() {
	let log = |index| LogId::new(CommittedLeaderId::new(1, 1), index);
	let entries: Vec<Entry<WendelonTypeConfig>> = vec![
		Entry {
			log_id: log(3),
			payload: EntryPayload::Blank,
		},
		Entry {
			log_id: log(4),
			payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![1, 2, 3])),
		},
		Entry {
			log_id: log(5),
			payload: EntryPayload::Membership(Membership::from(BTreeMap::from([(
				1u32,
				BasicNode { addr: "n1".into() },
			)]))),
		},
	];
	let mut s = rooted();
	for (raw, kind) in entries.iter().zip([0u8, 2, 1]) {
		let e = committed_entry2(&CommittedSource2::synthetic(), raw)
			.unwrap()
			.into_committed();
		let (position, identity, _) = e.entry_fields();
		s = covered_by(&s, &e);
		assert!(tip(&s) == (log2(position), identity, derived_digest(kind, identity)));
	}
	assert!(coverage2(&s) == Some(at(1, 1, 5)));
}

#[test]
fn cover_replay_is_idempotent_and_mismatch_refused() {
	let e3 = committed(3, 0x33, EntryClass2::Blank);
	let s = covered_by(&rooted(), &e3);
	// Exact replay: AlreadyProcessed, and the whole state unchanged.
	let (again, result) = advance_cover2(&s, &e3).unwrap();
	assert!(result == ApplyResult2::AlreadyProcessed);
	assert_same_state(&again, &s);
	// Same index, different identity.
	assert_eq!(
		advance_cover2(&s, &committed(3, 0x34, EntryClass2::Blank)).err(),
		Some(CoverError2::Root(RootError2::TrustMismatch))
	);
	// Same index, different class (so a different derived digest).
	assert_eq!(
		advance_cover2(&s, &committed(3, 0x33, EntryClass2::NonRoot2)).err(),
		Some(CoverError2::Root(RootError2::TrustMismatch))
	);
	// A skipped index.
	assert_eq!(
		advance_cover2(&s, &committed(5, 0x35, EntryClass2::Blank)).err(),
		Some(CoverError2::Root(RootError2::LineageGap))
	);
}

#[test]
fn malformed_and_command_classes_are_not_coverage() {
	let s = rooted();
	let before = encode_authority2(&s).unwrap();
	assert_eq!(
		advance_cover2(&s, &committed(3, 3, EntryClass2::MalformedRoot2)).err(),
		Some(CoverError2::MalformedAfterRoot)
	);
	assert_eq!(
		advance_cover2(&s, &committed(3, 3, EntryClass2::Bootstrap([1; 32]))).err(),
		Some(CoverError2::NotCoverable(EntryClass2Kind::Bootstrap))
	);
	assert_eq!(
		advance_cover2(&s, &committed(3, 3, EntryClass2::OtherRoot2([1; 32]))).err(),
		Some(CoverError2::NotCoverable(EntryClass2Kind::OtherRoot2))
	);
	assert_eq!(
		encode_authority2(&s).unwrap(),
		before,
		"the input state is never changed"
	);
	for pre in [empty_authority2(), genesis(), scanned()] {
		assert_eq!(
			advance_cover2(&pre, &committed(2, 2, EntryClass2::Blank)).err(),
			Some(CoverError2::NotRooted)
		);
	}
}

/// Several faults at once: the first check in the reducer's order decides (A1 r2 §2, note B).
#[test]
fn cover_precedence_with_multiple_faults() {
	let s = rooted(); // covered at (1,1,2)
	// A zero term or node at the covered index: the position check precedes same-index matching.
	for p in [at(0, 1, 2), at(1, 0, 2)] {
		assert_eq!(
			advance_cover2(
				&s,
				&CommittedEntry2::fixture(p, [9; 32], EntryClass2::Blank)
			)
			.err(),
			Some(CoverError2::Root(RootError2::Codec))
		);
	}
	// Same-index conflict is TrustMismatch, although the next-index predicate would also fail.
	assert_eq!(
		advance_cover2(&s, &committed(2, 9, EntryClass2::Blank)).err(),
		Some(CoverError2::Root(RootError2::TrustMismatch))
	);
	// State precedes class: a pre-root with a malformed entry is NotRooted.
	assert_eq!(
		advance_cover2(&scanned(), &committed(2, 9, EntryClass2::MalformedRoot2)).err(),
		Some(CoverError2::NotRooted)
	);
	// Class precedes the index checks: malformed at a gap index is MalformedAfterRoot.
	assert_eq!(
		advance_cover2(&s, &committed(9, 9, EntryClass2::MalformedRoot2)).err(),
		Some(CoverError2::MalformedAfterRoot)
	);
}

/// Synthetic, test-only tip at u64::MAX (no feasible history reaches it): replay, mismatch and
/// continuation behave exactly as at any other index; nothing wraps or panics.
#[test]
fn cover_at_index_max_is_a_lineage_gap() {
	let max = Log2 {
		term: 1,
		node: 1,
		index: u64::MAX,
	};
	let identity = [0x4d; 32];
	let s = with_tip_for_test(&rooted(), max, identity, derived_digest(0, identity));
	let exact = CommittedEntry2::fixture(at(1, 1, u64::MAX), identity, EntryClass2::Blank);
	let (again, result) = advance_cover2(&s, &exact).unwrap();
	assert!(result == ApplyResult2::AlreadyProcessed);
	assert_same_state(&again, &s);
	assert_eq!(
		advance_cover2(
			&s,
			&CommittedEntry2::fixture(at(1, 1, u64::MAX), [0x4e; 32], EntryClass2::Blank)
		)
		.err(),
		Some(CoverError2::Root(RootError2::TrustMismatch))
	);
	assert_eq!(
		advance_cover2(&s, &committed(5, 5, EntryClass2::Blank)).err(),
		Some(CoverError2::Root(RootError2::LineageGap))
	);
}

#[test]
fn cover_zero_coordinate_is_refused() {
	let s = rooted();
	for p in [at(0, 1, 3), at(1, 0, 3)] {
		assert_eq!(
			advance_cover2(
				&s,
				&CommittedEntry2::fixture(p, [3; 32], EntryClass2::Blank)
			)
			.err(),
			Some(CoverError2::Root(RootError2::Codec))
		);
	}
}

/// The no-effects oracle (A1 r2 §5, note C), from the bootstrap tip and from a later tip.
#[test]
fn cover_cannot_reach_effects() {
	let at_bootstrap = rooted();
	let later = covered_by(&at_bootstrap, &committed(3, 0x33, EntryClass2::NonRoot2));
	for (before, next) in [
		(at_bootstrap, committed(3, 0x31, EntryClass2::Blank)),
		(later, committed(4, 0x44, EntryClass2::Membership)),
	] {
		let (after, result) = advance_cover2(&before, &next).unwrap();
		assert!(result == ApplyResult2::Covered);
		let (position, identity, _) = next.entry_fields();
		let kind = if next.entry_fields().2 == EntryClass2::Blank {
			0
		} else {
			1
		};
		assert!(tip(&after) == (log2(position), identity, derived_digest(kind, identity)));
		// Only covered, fingerprint and command_digest differ: normalise them to after's.
		let (c, f, d) = tip(&after);
		let normalized = with_tip_for_test(&before, c, f, d);
		assert_same_state(&normalized, &after);
	}
}

/// Under a test-only lowered limit, an advancing cover follows `reduce` exactly (A1 r2 §3,
/// note A): replay still answers before any capacity check, and the next index fails capacity
/// for both paths alike.
#[test]
fn cover_limit_follows_reduce_under_lowered_limits() {
	let tight = Limits2 {
		slots: 0,
		..Limits2::default()
	};
	let r = root("root_reserved");
	assert!(
		r.state.derived.usage.parents > 0,
		"the fixture holds a parent"
	);
	let (g, c) = (r.state.generation, r.state.cluster);
	let next = l(r.covered().index + 1);
	let fp = [0x5c; 32];
	// Replay on the lowered root: AlreadyProcessed, before capacity.
	let (covered, result) = Root2::cover(&r, next, fp, 0).unwrap();
	assert!(result == ApplyResult2::Covered);
	let (_, replay) = Root2::cover(&covered.clone().lower_limits(tight), next, fp, 0).unwrap();
	assert!(replay == ApplyResult2::AlreadyProcessed);
	// The next index on the lowered root: capacity refuses, exactly as reduce does.
	let lowered = r.clone().lower_limits(tight);
	assert_eq!(
		Root2::cover(&lowered, next, fp, 0).err(),
		Some(RootError2::Limit)
	);
	let command = Command2::Cover {
		kind: 0,
		external: fp,
	};
	let e = TrustedEntry2::fixture(g, c, next, &command).unwrap();
	let v = VerifiedCommand2::fixture(g, c, &command, &[], 0).unwrap();
	assert_eq!(
		Root2::reduce(Some(&lowered), &e, &v, &command).err(),
		Some(RootError2::Limit)
	);
}

#[test]
fn bootstrap_root_satisfies_capacity_invariant() {
	let s = rooted();
	let root = root_of(&s);
	assert!(cached_capacity(&root.state, Limits2::default()).is_ok());
	assert_eq!(root.state.derived.nodes.len(), 0);
	assert!(cached_capacity(&root.state, root.limits).is_ok());
	assert_eq!(limits(root.limits), limits(Limits2::default()));
}

/// T-S4's pure target tuple: derived from the entry at the frontier (here a current-term noop
/// Blank), it equals the tip once coverage has advanced through the frontier, and replay is
/// Ready there; stopping one short is never Ready. Quorum acquisition is not modelled.
#[test]
fn frontier_target_equals_covered_tip() {
	let frontier = committed(3, 0x3f, EntryClass2::Blank);
	let (position, identity, _) = frontier.entry_fields();
	let target = ReplayTarget2 {
		cut: log2(position),
		fingerprint: identity,
		command: crate::storage::mutation_visibility::root2::recovery::TargetCommand2::Expected(
			derived_digest(0, identity),
		),
	};
	let deployment = Deployment2 {
		generation: G,
		cluster: C,
	};
	let states = [genesis(), scanned(), rooted()];
	let through = covered_by(&states[2], &frontier);
	assert!(tip(&through) == (target.cut, target.fingerprint, match target.command { crate::storage::mutation_visibility::root2::recovery::TargetCommand2::Expected(d) => d, _ => unreachable!() }));
	let mut m = replay_monitor2(deployment, Some(Baseline2::Provisioned), target).unwrap();
	for s in &states {
		assert!(matches!(observe2(&mut m, s).unwrap(), Progress2::Replaying));
	}
	assert!(matches!(
		observe2(&mut m, &through).unwrap(),
		Progress2::Ready(_)
	));
	// One short: never Ready.
	let mut short = replay_monitor2(deployment, Some(Baseline2::Provisioned), target).unwrap();
	for s in &states {
		assert!(!matches!(
			observe2(&mut short, s).unwrap(),
			Progress2::Ready(_)
		));
	}
}
