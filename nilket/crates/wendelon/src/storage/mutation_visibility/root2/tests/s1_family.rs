//! T-FAMILY (D-G1): the profile-3 S1 twins are what a real origin reaches. Every history in the
//! generator's provenance map (`goldens.json` "provenance", from `goldens_profile3_reference.py`)
//! is replayed from genesis: the Bootstrap at (1,1,1) through `bootstrap2`, the AdmissionPermission
//! at 4 through `advance_admission2` (production statement checks, over `M3_S1`), and every other
//! step through `advance_root2`, all with fixture verification tokens. Each replayed tip must carry
//! the golden's `covered` and `command_digest`; after only the tip fingerprint is normalized to the
//! golden placeholder, the root must equal the golden byte for byte. Each result, record and command
//! golden must equal what its provenance step produced.
use super::profile_three_fixture::{M3_S1, tuple_of};
use super::*;
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_install::observe_bootstrap2;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::PinnedBoundTuple2;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use std::collections::BTreeMap;

const G: [u8; 16] = [0x11; 16];
const C: [u8; 16] = [0x12; 16];
const PLACEHOLDER: Digest2 = [0xf1; 32];

fn provenance() -> serde_json::Value {
	let j: serde_json::Value = serde_json::from_str(include_str!("goldens.json")).unwrap();
	j["provenance"].clone()
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
/// The flattened step list of a history, ancestors first.
fn steps(p: &serde_json::Value, history: &str) -> Vec<String> {
	let h = &p["histories"][history];
	assert!(h.is_object(), "unknown history {history}");
	let mut out = match h["from"].as_str() {
		Some(parent) => steps(p, parent),
		None => Vec::new(),
	};
	out.extend(
		h["steps"]
			.as_array()
			.unwrap()
			.iter()
			.map(|s| s.as_str().unwrap().to_string()),
	);
	out
}
fn root_of(state: &MutationAuthorityState2) -> Root2 {
	root_and_origin2(state).unwrap().0.clone()
}

struct Replay {
	state: MutationAuthorityState2,
	/// The result of each step, by the index it processed.
	results: BTreeMap<u64, ApplyResult2>,
	/// The root after each step, by the index it processed.
	roots: BTreeMap<u64, Root2>,
}

/// Genesis at index 0 and the Bootstrap at (1,1,1), then the observed Bound tuple over `M3_S1`.
fn bootstrapped(
	p: &serde_json::Value,
) -> (MutationAuthorityState2, ApplyResult2, PinnedBoundTuple2) {
	let genesis: Digest2 = unhex(p["genesis_s1"].as_str().unwrap()).try_into().unwrap();
	let first = CommittedEntry2::fixture(at(0, 0, 0), genesis, EntryClass2::Membership);
	let (pre, step) = advance_pre_root2(&empty_authority2(), &first).unwrap();
	assert_eq!(step, PreRootStep2::Genesis);
	let cmd = command("command_bootstrap_p3");
	let e = TrustedEntry2::fixture(G, C, l(1), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let v = VerifiedCommand2::fixture(G, C, &cmd, &[], 0).unwrap();
	let committed = CommittedEntry2::fixture(at(1, 1, 1), fp, EntryClass2::Bootstrap(d));
	let (state, result) = bootstrap2(&pre, &committed, &e, &v, &cmd).unwrap();
	let origin = root_and_origin2(&state).unwrap().1;
	assert_eq!(
		(
			origin.genesis,
			origin.bootstrap_fingerprint,
			origin.bootstrap_command
		),
		(genesis, fp, d)
	);
	let observation = observe_bootstrap2(&state).unwrap();
	let tuple = tuple_of(
		&M3_S1,
		BootstrapRecord2 {
			identity: observation.identity,
			inventory_seq: 1,
		},
	);
	(state, result, tuple)
}

fn replay(p: &serde_json::Value, history: &str) -> Replay {
	let all = steps(p, history);
	assert_eq!(all[0], "command_bootstrap_p3", "{history}");
	let (mut state, result, tuple) = bootstrapped(p);
	let mut results = BTreeMap::from([(1, result)]);
	let mut roots = BTreeMap::from([(1, root_of(&state))]);
	let mut last: Option<(TrustedEntry2, VerifiedCommand2, Command2)> = None;
	for name in &all[1..] {
		let tip = root_of(&state);
		let i = tip.covered().index + 1;
		let (next, result, index) = match name.as_str() {
			// The exact tip entry, applied again.
			"replay_tip" => {
				let (e, v, cmd) = last.as_ref().unwrap();
				let (next, result) = advance_root2(&state, e, v, cmd).unwrap();
				(next, result, i - 1)
			}
			// A Reserve of a new identity on a root with no free parent slot.
			"reserve_without_slots" => {
				let mut c = claim();
				c.manifest.identity.sequence += 1;
				let cmd = Command2::Reserve(c);
				let e = TrustedEntry2::fixture(G, C, l(i), &cmd).unwrap();
				let v = VerifiedCommand2::fixture(G, C, &cmd, &[original()], 1).unwrap();
				let mut limits = Limits2::default();
				limits.slots = 0;
				let (_, result) =
					Root2::reduce(Some(&tip.lower_limits(limits)), &e, &v, &cmd).unwrap();
				results.insert(i, result);
				continue;
			}
			_ => {
				let cmd = match name.as_str() {
					// The golden Finalize with a one-node acknowledgement: below the plan's quorum.
					"finalize_one_ack" => {
						let mut c = finalized(&tip);
						if let Command2::Finalize { attestation, .. } = &mut c {
							attestation.children[0].nodes.truncate(1);
						}
						c
					}
					// The golden Finalize whose attestation names another claim digest.
					"finalize_foreign_claim_digest" => {
						let mut c = finalized(&tip);
						if let Command2::Finalize { attestation, .. } = &mut c {
							attestation.claim_digest[0] ^= 1;
						}
						c
					}
					golden => command(golden),
				};
				let trace = match cmd {
					Command2::Reserve(_) => vec![original()],
					_ => Vec::new(),
				};
				let e = TrustedEntry2::fixture(G, C, l(i), &cmd).unwrap();
				let v = VerifiedCommand2::fixture(G, C, &cmd, &trace, 1).unwrap();
				let (next, result) = if matches!(cmd, Command2::AdmissionPermission(_)) {
					advance_admission2(&state, &tuple, &e, &v, &cmd)
				} else {
					advance_root2(&state, &e, &v, &cmd)
				}
				.unwrap_or_else(|err| panic!("{history}: {name} at {i}: {err:?}"));
				last = Some((e, v, cmd));
				(next, result, i)
			}
		};
		state = next;
		results.insert(index, result);
		roots.insert(index, root_of(&state));
	}
	Replay {
		state,
		results,
		roots,
	}
}

#[test]
fn s1_family_replays_every_parent_lineage_golden_from_genesis() {
	let p = provenance();
	let histories = p["histories"].as_object().unwrap();
	let mut replays = BTreeMap::new();
	for name in histories.keys() {
		replays.insert(name.clone(), replay(&p, name));
	}
	// Roots: covered and command digest as replayed, then only the tip fingerprint normalized.
	let mut roots = 0;
	for (name, h) in histories {
		let Some(golden) = h["root"].as_str() else {
			continue;
		};
		let r = &replays[name];
		let tip = root_of(&r.state);
		let want = root(golden);
		assert!(tip.covered() == want.covered(), "{golden}");
		assert_eq!(
			tip.state.command_digest, want.state.command_digest,
			"{golden}"
		);
		assert_ne!(tip.state.fingerprint, PLACEHOLDER, "{golden}");
		let normalized = with_tip_for_test(
			&r.state,
			tip.covered(),
			PLACEHOLDER,
			tip.state.command_digest,
		);
		assert_eq!(
			root_of(&normalized).encode().unwrap(),
			fixture(golden),
			"{golden}"
		);
		roots += 1;
	}
	assert_eq!(roots, 7);
	// Every other provenance entry: the result, record or command its step produced.
	let mut checked = 0;
	for (name, at) in p["fixtures"].as_object().unwrap() {
		let history = at[0].as_str().unwrap();
		let index = at[1].as_u64().unwrap();
		let r = &replays[history];
		let golden = fixture(name);
		let after = || r.roots[&index].clone();
		let identity = claim().manifest.identity;
		let produced = if name.starts_with("result_") {
			r.results[&index].encode(G, C, l(index)).unwrap()
		} else if name.starts_with("command_") {
			assert_eq!(steps(&p, history)[index as usize - 1], *name, "{name}");
			command(name).encode(G, C).unwrap()
		} else {
			match name.as_str() {
				"grant_ref2" => lineage(&after())
					.latest
					.reference()
					.unwrap()
					.encode()
					.unwrap(),
				"original_grant2" => lineage(&after()).latest.encode().unwrap(),
				"lineage_record_open" | "lineage_record_renewed" => {
					lineage(&after()).encode().unwrap()
				}
				"claim_projection2" => after()
					.lookup(&identity)
					.unwrap()
					.unwrap()
					.claim
					.encode()
					.unwrap(),
				"parent_reserved" | "parent_accepted" | "parent_refused_cut" => after()
					.lookup(&identity)
					.unwrap()
					.unwrap()
					.encode()
					.unwrap(),
				"holder_attestation1" => match command("command_finalize") {
					Command2::Finalize { attestation, .. } => attestation.encode().unwrap(),
					_ => panic!("command_finalize is a Finalize"),
				},
				"epoch_cut2" => after().state.cuts.iter().next().unwrap().encode().unwrap(),
				other => panic!("no provenance check for {other}"),
			}
		};
		assert_eq!(produced, golden, "{name} at {history}[{index}]");
		checked += 1;
	}
	assert_eq!(checked, 31);
}
fn lineage(r: &Root2) -> LineageRecord2 {
	let mut all = r.state.lineages.values();
	let g = all.next().unwrap().clone();
	assert!(all.next().is_none());
	g
}
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
